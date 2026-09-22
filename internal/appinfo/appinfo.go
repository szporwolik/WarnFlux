// Package appinfo holds a handful of application identity constants shared
// by packages that must not depend on each other (web templates, contact
// actions such as smtp).
package appinfo

import _ "embed"

// RepoURL is the public repository of this application. It is linked from
// the web footer and from outbound notification footers.
const RepoURL = "https://github.com/szporwolik/WarnFlux"

// logoPNG is the application logo embedded for outbound email branding.
// It mirrors internal/web/static/assets/logo.png (128x128, the resized UI
// variant of the master asset in /assets).
//
//go:embed logo.png
var logoPNG []byte

// LogoPNG returns the embedded application logo (PNG, 128x128).
func LogoPNG() []byte { return logoPNG }
