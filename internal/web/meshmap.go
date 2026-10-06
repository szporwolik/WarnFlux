package web

import "net/http"

// handleMeshMapPage redirects to the combined Meshtastic section: the
// node map now lives on the map tab of /meshtastic. Kept so old links
// and bookmarks keep working.
func (s *Server) handleMeshMapPage(w http.ResponseWriter, r *http.Request) {
	target := "/meshtastic?tab=map"
	if q := r.URL.RawQuery; q != "" {
		target += "&" + q
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}
