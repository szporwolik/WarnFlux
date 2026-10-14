package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/szporwolik/WarnFlux/internal/burzedzis"
	"github.com/szporwolik/WarnFlux/internal/config"
	"github.com/szporwolik/WarnFlux/internal/i18n"
	stormsource "github.com/szporwolik/WarnFlux/internal/plugins/sources/burzedzis"
)

// stormLookupTimeout bounds the POST-time locality check against the
// provider. The check is a convenience, never a gate: a timeout saves the
// list and says it could not be verified.
const stormLookupTimeout = 15 * time.Second

// stormView is the storm-localities section of the Config page: one row per
// burze.dzis.net source instance, with the localities the file holds.
type stormView struct {
	// Sources is one row per configured storm source instance.
	Sources []stormSourceRow
	// Max is the accepted number of localities per instance.
	Max int
	// MaxLen is the accepted length of one locality name.
	MaxLen int
}

// stormSourceRow is one storm source instance.
type stormSourceRow struct {
	// Index is the row position (the handler reads path_N / locations_N).
	Index int
	// Title names the instance ("burze-main (burzedzis)").
	Title string
	// Path is the dotted YAML path of the localities list.
	Path string
	// Field is the comma-separated list the field shows.
	Field string
	// Names are the configured localities, as the chips show them.
	Names []string
	// HasKey reports whether the instance carries an API key, i.e. whether
	// a submitted list can be checked against the provider's directory.
	HasKey bool
}

// buildStormView renders the section from the CONFIGURATION FILE (the panel
// shows the operator what is really in the file, not what the running
// process happens to hold).
func (s *Server) buildStormView() stormView {
	v := stormView{
		Max:    stormsource.MaxLocalities,
		MaxLen: stormsource.MaxLocalityNameLen,
	}
	if s.configPath == "" {
		return v
	}
	insts, err := config.Instances(s.configPath, "sources")
	if err != nil {
		s.logger.Warn("config: source instances unreadable", "error", err)
		return v
	}
	for _, in := range insts {
		if in.Type != stormsource.Type {
			continue
		}
		base := in.ConfigPath("sources")
		names, _ := config.StringListAt(s.configPath, base+".locations")
		title := in.ID
		if title == "" {
			title = fmt.Sprintf("#%d", in.Index)
		}
		key, _ := config.ScalarAt(s.configPath, base+".api_key")
		keyFile, _ := config.ScalarAt(s.configPath, base+".api_key_file")
		v.Sources = append(v.Sources, stormSourceRow{
			Index:  len(v.Sources),
			Title:  fmt.Sprintf("%s (%s)", title, in.Type),
			Path:   base + ".locations",
			Field:  strings.Join(names, ", "),
			Names:  names,
			HasKey: strings.TrimSpace(key) != "" || strings.TrimSpace(keyFile) != "",
		})
	}
	return v
}

// splitLocalities splits the field into names: commas, semicolons and
// newlines all separate, so a pasted column of towns works as well as a
// typed list.
func splitLocalities(raw string) []string {
	return strings.FieldsFunc(raw, func(r rune) bool {
		switch r {
		case ',', ';', '\n', '\r', '\t':
			return true
		}
		return false
	})
}

// handleConfigStorm applies the storm-localities section. The names are
// checked against the provider's own directory before they are written: the
// directory is diacritic-sensitive and answers 0/0 for a town it does not
// know — the very coordinates that make the warnings endpoint return sample
// data — so a typo would otherwise turn into silence (or into fake alerts).
// An unreachable provider does not block the save; it is reported instead.
func (s *Server) handleConfigStorm(w http.ResponseWriter, r *http.Request) {
	sess := s.sessions.currentSession(r)
	if err := r.ParseForm(); err != nil || !s.requireStateChange(w, r, sess) {
		if err == nil {
			return
		}
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	lang := s.langFor(r)
	view := s.buildStormView()
	if len(view.Sources) == 0 {
		http.Redirect(w, r, "/config?msg=storm-none", http.StatusSeeOther)
		return
	}

	var edits []config.ScalarEdit
	unverified := false
	for _, row := range view.Sources {
		raw := r.PostFormValue(fmt.Sprintf("locations_%d", row.Index))
		path := strings.TrimSpace(r.PostFormValue(fmt.Sprintf("path_%d", row.Index)))
		if path != row.Path {
			// The form carries its own path so a disabled row cannot shift
			// the others; anything else is a stale page.
			s.renderConfigError(w, r, http.StatusUnprocessableEntity, i18n.T(lang, "config.storm.stale"))
			return
		}
		if len(raw) > contentFieldCap {
			s.renderConfigError(w, r, http.StatusBadRequest,
				i18nFieldError(lang, "config.storm.field", fmt.Errorf("maximum %d bytes", contentFieldCap)))
			return
		}
		names, err := stormsource.NormalizeLocations(splitLocalities(raw))
		if err != nil {
			s.renderConfigError(w, r, http.StatusUnprocessableEntity,
				i18nFieldError(lang, "config.storm.field", err))
			return
		}
		if sameList(row.Names, names) {
			continue
		}
		if row.HasKey {
			unknown, ok := s.checkStormLocalities(r, row.Path, names)
			if !ok {
				unverified = true
			} else if len(unknown) > 0 {
				s.renderConfigError(w, r, http.StatusUnprocessableEntity,
					fmt.Sprintf(i18n.T(lang, "config.storm.unknown"), strings.Join(unknown, ", ")))
				return
			}
		} else {
			unverified = true
		}
		edits = append(edits, config.ScalarEdit{Path: row.Path, Value: config.StringListScalar(names)})
	}

	if len(edits) == 0 {
		http.Redirect(w, r, "/config?msg=storm-none-changes", http.StatusSeeOther)
		return
	}

	perr := ""
	if !s.persistConfigYAML("storm localities", func() error {
		return s.updateConfigFile(edits)
	}) {
		perr = "&perr=1"
	}
	s.audit(sess.username, "config-storm", fmt.Sprintf("instances=%d unverified=%v", len(edits), unverified))
	s.logger.Info("storm localities updated by admin", "user", sess.username, "instances", len(edits), "verified", !unverified)

	msg := "storm"
	if unverified {
		msg = "storm-unverified"
	}
	http.Redirect(w, r, "/config?msg="+msg+perr, http.StatusSeeOther)
}

// checkStormLocalities resolves the submitted names through the provider's
// directory. ok is false when the check could not run at all (no key, no
// network, the provider refusing the call) — the caller then saves the list
// and says it is unverified.
func (s *Server) checkStormLocalities(r *http.Request, path string, names []string) (unknown []string, ok bool) {
	key, _ := s.scalarAt(strings.TrimSuffix(path, ".locations") + ".api_key")
	keyFile, _ := s.scalarAt(strings.TrimSuffix(path, ".locations") + ".api_key_file")
	baseURL, _ := s.scalarAt(strings.TrimSuffix(path, ".locations") + ".base_url")
	key = strings.TrimSpace(key)
	if key == "" {
		data, err := readSecretFile(strings.TrimSpace(keyFile))
		if err != nil {
			s.logger.Warn("config: storm api key unreadable", "error", err)
			return nil, false
		}
		key = data
	}
	if key == "" {
		return nil, false
	}
	base := strings.TrimSpace(baseURL)
	if base == "" {
		base = burzedzis.DefaultBaseURL
	}

	ctx, cancel := context.WithTimeout(r.Context(), stormLookupTimeout)
	defer cancel()
	client := burzedzis.NewClient(base, key, stormLookupTimeout)
	for _, name := range names {
		if _, err := client.Locality(ctx, name); err != nil {
			if !errors.Is(err, burzedzis.ErrUnknownLocality) {
				// A refusal, a timeout or a decode failure is not a typo:
				// the save proceeds and is reported as unverified.
				s.logger.Warn("config: storm locality check unavailable", "locality", name, "error", err)
				return nil, false
			}
			unknown = append(unknown, name)
		}
	}
	return unknown, true
}

// readSecretFile reads a small secret file (the optional api_key_file),
// bounded and trimmed the way the plugin's loader does it.
func readSecretFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.Size() > 64*1024 {
		return "", fmt.Errorf("secret file is %d bytes, maximum %d", info.Size(), 64*1024)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// sameList reports whether the submitted names are what the file already
// holds, so saving a form without touching the field writes nothing.
func sameList(current, submitted []string) bool {
	if len(current) != len(submitted) {
		return false
	}
	for i := range current {
		if current[i] != submitted[i] {
			return false
		}
	}
	return true
}
