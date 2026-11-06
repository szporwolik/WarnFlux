package web

import (
	"github.com/szporwolik/WarnFlux/internal/plugins/sources/gddkiadatex"
)

// roadText renders the stored headline of one hazard for a reader, and keeps
// a description that merely repeated the old headline in step with it.
//
// GDDKiA DATEX records written before the language-free headline carried the
// provider's English label inside the text ("Road works — road A4, km 8.7").
// The stored rows keep that shape for as long as an alert lives in the
// retention window, so the rewrite happens on the way out — otherwise a
// Polish reader would still meet the English label in the archive and in the
// detail popup, and would see the type twice.
func roadText(source, headline, description string) (string, string) {
	if source != "gddkia-datex" || headline == "" {
		return headline, description
	}
	legacy := headline
	headline = gddkiadatex.LegacyHeadline(headline)
	// When the record carried no provider text, the adapter repeated the
	// headline into the description: that copy has to follow the rewrite.
	if description == legacy {
		description = headline
	}
	return headline, description
}
