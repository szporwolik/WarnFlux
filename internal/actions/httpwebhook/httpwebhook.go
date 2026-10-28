// Package httpwebhook implements the generic HTTP webhook action: one
// JSON POST per routed hazard notification. A single plugin type covers
// Discord, Slack-compatible gateways, ntfy, Gotify bridges, Home
// Assistant and SMS gateways — anything that accepts a webhook POST.
//
// It follows the ActionPlugin architecture exactly: YAML config decoding,
// factory validation (a missing secret or an invalid configuration is a
// startup error, never a runtime surprise), a per-call bounded HTTP
// request, and a no-op Close. Delivery retries are owned by the action
// instance machinery (runtime.retries), not by this plugin.
package httpwebhook

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/szporwolik/WarnFlux/internal/action"
)

// Type is the action type name used in the YAML configuration.
const Type = "http_webhook"

const (
	// maxSecretFileBytes bounds the token file read at construction.
	maxSecretFileBytes = 64 * 1024
	// defaultTimeout bounds one request when the config omits it. The
	// action instance's call_timeout still caps the whole call.
	defaultTimeout = 10 * time.Second

	// maxLocationPath caps the path shown when a redirect is reported: a
	// path is useful diagnostics, an 80-character one is enough.
	maxLocationPath = 80

	// maxHeaders bounds the configured header map.
	maxHeaders = 32
)

// Config is the action-specific configuration.
type Config struct {
	// URL is the webhook endpoint (http or https). Required.
	URL string `yaml:"url"`
	// Headers are additional request headers (e.g. content-type
	// overrides or provider-specific fields).
	Headers map[string]string `yaml:"headers"`
	// Token is the bearer secret. Mutually exclusive with TokenFile.
	Token string `yaml:"token"`
	// TokenFile reads the bearer secret from a file (Docker secret).
	// Mutually exclusive with Token.
	TokenFile string `yaml:"token_file"`
	// Timeout bounds one HTTP request; 0 falls back to defaultTimeout.
	Timeout time.Duration `yaml:"timeout"`
}

// webhookPayload is the JSON body of one delivery, built from the
// canonical dispatch event plus application identity.
type webhookPayload struct {
	SchemaVersion int    `json:"schema_version"`
	Type          string `json:"type"`
	RequestID     string `json:"request_id"`
	ChangeType    string `json:"change_type"`
	EventKey      string `json:"event_key"`
	Event         struct {
		Source           string   `json:"source"`
		SourceID         string   `json:"source_id"`
		Event            string   `json:"event"`
		Severity         string   `json:"severity"`
		ProviderSeverity string   `json:"provider_severity,omitempty"`
		Urgency          string   `json:"urgency"`
		Certainty        string   `json:"certainty"`
		Headline         string   `json:"headline"`
		Areas            []string `json:"areas"`
		EffectiveAt      *string  `json:"effective_at,omitempty"`
		ExpiresAt        *string  `json:"expires_at,omitempty"`
		ReceivedAt       string   `json:"received_at"`
		UpdatedAt        string   `json:"updated_at"`
	} `json:"event"`
	App struct {
		Version string `json:"version"`
		Header1 string `json:"header1"`
		Domain  string `json:"domain"`
		RepoURL string `json:"repo_url"`
	} `json:"app"`
}

type webhookAction struct {
	cfg       Config
	url       *url.URL
	token     string
	timeout   time.Duration
	client    *http.Client
	canonical map[string]string // lowercased configured headers
	logger    *slog.Logger
}

// New decodes and validates the configuration and builds the action. A
// missing URL, a non-http(s) URL, both token variants set, or a missing
// token file is a construction error.
func New(node *yaml.Node) (action.Plugin, error) {
	var cfg Config
	if node != nil {
		if err := node.Decode(&cfg); err != nil {
			return nil, fmt.Errorf("http_webhook: decode config: %w", err)
		}
	}

	if strings.TrimSpace(cfg.URL) == "" {
		return nil, fmt.Errorf("http_webhook: config.url is required")
	}
	u, err := url.Parse(strings.TrimSpace(cfg.URL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("http_webhook: config.url must be an absolute http(s) URL")
	}
	if cfg.Token != "" && cfg.TokenFile != "" {
		return nil, fmt.Errorf("http_webhook: config.token and config.token_file are mutually exclusive")
	}
	if len(cfg.Headers) > maxHeaders {
		return nil, fmt.Errorf("http_webhook: config.headers has %d entries, maximum %d", len(cfg.Headers), maxHeaders)
	}
	for name := range cfg.Headers {
		if strings.TrimSpace(name) == "" || strings.ContainsAny(name, "\r\n") {
			return nil, fmt.Errorf("http_webhook: config.headers contains an invalid header name %q", name)
		}
		if strings.ContainsAny(cfg.Headers[name], "\r\n") {
			return nil, fmt.Errorf("http_webhook: config.headers[%q] contains a newline", name)
		}
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	p := &webhookAction{
		cfg:     cfg,
		url:     u,
		timeout: timeout,
		client: &http.Client{
			Timeout: timeout,
			// A webhook is delivered by the response to THIS POST. With
			// the default policy a 3xx is followed as a bodyless GET: an
			// endpoint that sent the client to a login page answered 200
			// and the alert was reported delivered although it never
			// arrived (reported P2). The redirect is surfaced instead.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		logger: slog.Default(),
	}
	if cfg.TokenFile != "" {
		info, err := os.Stat(cfg.TokenFile)
		if err != nil {
			return nil, fmt.Errorf("http_webhook: stat config.token_file: %w", err)
		}
		if info.Size() > maxSecretFileBytes {
			return nil, fmt.Errorf("http_webhook: config.token_file is %d bytes, maximum %d", info.Size(), maxSecretFileBytes)
		}
		data, err := os.ReadFile(cfg.TokenFile)
		if err != nil {
			return nil, fmt.Errorf("http_webhook: read config.token_file: %w", err)
		}
		p.token = strings.TrimRight(string(data), "\r\n")
	} else {
		p.token = strings.TrimSpace(cfg.Token)
	}
	p.canonical = make(map[string]string, len(cfg.Headers))
	for name, value := range cfg.Headers {
		p.canonical[http.CanonicalHeaderKey(name)] = value
	}
	return p, nil
}

// Name returns the action type name.
func (p *webhookAction) Name() string { return Type }

// SharedDestination implements the optional capability: the payload goes to
// ONE configured endpoint and is built from the event alone (no per-group
// recipients), so two matching groups would POST the same body twice to the
// same URL and trigger whatever the webhook drives twice (reported P2).
func (p *webhookAction) SharedDestination() bool { return true }

// Execute delivers one webhook POST. Non-2xx responses (redirects
// included — see the redirect policy) and transport errors are returned
// as errors; the action instance machinery owns retries.
func (p *webhookAction) Execute(ctx context.Context, req action.ActionRequest) error {
	payload := p.buildPayload(req)
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("http_webhook: marshal payload: %w", err)
	}

	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("http_webhook: build request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	if ua := strings.TrimSpace(req.App.Version); ua != "" {
		hreq.Header.Set("User-Agent", "WarnFlux/"+ua)
	} else {
		hreq.Header.Set("User-Agent", "WarnFlux")
	}
	if p.token != "" {
		hreq.Header.Set("Authorization", "Bearer "+p.token)
	}
	for name, value := range p.canonical {
		hreq.Header.Set(name, value)
	}

	resp, err := p.client.Do(hreq)
	if err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("http_webhook: request cancelled: %w", ctx.Err())
		}
		return p.transportError(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4*1024))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		if loc := redirectLocation(resp); loc != "" {
			return fmt.Errorf("http_webhook: endpoint answered %s (redirect to %s), so the POST was not delivered — point config.url at the webhook itself",
				resp.Status, loc)
		}
		return fmt.Errorf("http_webhook: endpoint answered %s", resp.Status)
	}
	return nil
}

// redirectLocation reports where a 3xx response wanted to send the request
// (empty for other responses). The value comes from the endpoint, so it is
// treated as untrusted TEXT: only the scheme and host survive, because a
// redirect target routinely carries its own credential in the path or the
// query, and both ended up in error lines and logs.
func redirectLocation(resp *http.Response) string {
	if resp.StatusCode < 300 || resp.StatusCode > 399 {
		return ""
	}
	loc := strings.TrimSpace(resp.Header.Get("Location"))
	if loc == "" {
		return ""
	}
	u, err := url.Parse(loc)
	if err != nil || (u.Host == "" && u.Path == "") {
		return "a location that is not a URL"
	}
	// Keep the HOST and the PATH (they tell the operator where the POST was
	// sent — a login page, another hook) and drop everything that routinely
	// carries a credential: query, fragment and userinfo.
	path := u.Path
	if len(path) > maxLocationPath {
		path = path[:maxLocationPath] + "…"
	}
	shown := path
	if u.Host != "" {
		shown = u.Scheme + "://" + u.Host + path
	}
	if u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		shown += " (query, fragment and credentials omitted)"
	}
	return shown
}

// endpointID is the safe identifier an error names instead of the URL: the
// scheme and host plus a short hash of the full URL. The hash keeps two
// webhooks on the same host distinguishable without printing either path —
// webhook URLs are frequently their own credential ("…/hook/T0KEN?key=…").
func (p *webhookAction) endpointID() string {
	if p.url == nil {
		return "the configured endpoint"
	}
	sum := sha256.Sum256([]byte(p.url.String()))
	return fmt.Sprintf("%s://%s#%s", p.url.Scheme, p.url.Host, hex.EncodeToString(sum[:4]))
}

// transportError renders a failed request without leaking the URL. A
// *url.Error prints the full URL twice (in Op output and in the wrapped
// error), so the cause is rebuilt from its parts: the operation, the safe
// endpoint identifier and the underlying error, which carries no URL.
func (p *webhookAction) transportError(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		return fmt.Errorf("http_webhook: request to %s failed during %s: %w",
			p.endpointID(), uerr.Op, uerr.Err)
	}
	return fmt.Errorf("http_webhook: request to %s failed: %w", p.endpointID(), err)
}

// Close releases nothing (stateless per-call client).
func (p *webhookAction) Close(ctx context.Context) error {
	_ = ctx
	p.client.CloseIdleConnections()
	return nil
}

// buildPayload assembles the JSON body from the canonical dispatch event.
func (p *webhookAction) buildPayload(req action.ActionRequest) webhookPayload {
	var w webhookPayload
	w.SchemaVersion = 1
	w.Type = "hazard_notification"
	w.RequestID = req.ID

	h := req.Event.Hazard
	if h == nil {
		return w
	}
	w.ChangeType = string(h.Type)
	w.EventKey = h.Key
	w.Event.Source = h.Hazard.Source
	w.Event.SourceID = h.Hazard.SourceID
	w.Event.Event = h.Hazard.Event
	w.Event.Severity = h.Hazard.Severity
	w.Event.ProviderSeverity = h.Hazard.ProviderSeverity
	w.Event.Urgency = h.Hazard.Urgency
	w.Event.Certainty = h.Hazard.Certainty
	w.Event.Headline = h.Hazard.Headline
	w.Event.Areas = append([]string(nil), h.Hazard.Areas...)
	w.Event.ReceivedAt = formatRFC3339(h.Hazard.ReceivedAt)
	w.Event.UpdatedAt = formatRFC3339(h.Hazard.UpdatedAt)
	if h.Hazard.EffectiveAt != nil {
		s := formatRFC3339(*h.Hazard.EffectiveAt)
		w.Event.EffectiveAt = &s
	}
	if h.Hazard.ExpiresAt != nil {
		s := formatRFC3339(*h.Hazard.ExpiresAt)
		w.Event.ExpiresAt = &s
	}

	w.App.Version = req.App.Version
	w.App.Header1 = req.App.Header1
	w.App.Domain = req.App.Domain
	w.App.RepoURL = req.App.RepoURL
	return w
}

// formatRFC3339 renders an optional timestamp; zero times become "".
func formatRFC3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
