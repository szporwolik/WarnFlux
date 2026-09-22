// Package rso implements the Regionalny System Ostrzegania (RSO) public
// XML source: https://komunikaty.tvp.pl (komunikaty.xml integration).
//
// The list endpoint /komunikatyxml/{wojewodztwo}/{kategoria}/{page}
// with page=0 returns the full current applicable communication set for
// the queried voivodeship (verified live); filtering happens UPSTREAM
// through the official URL.
package rso

import "encoding/xml"

// newsList is the expected root document (verified live):
//
//	<newses>
//	  <pagination_info totalItems="136" itemsPerPage="20"></pagination_info>
//	  <news> ... </news>
//	</newses>
//
// Unmarshalling into a struct with the expected root name fails for any
// other document (HTML error page, unrelated XML, truncated XML).
type newsList struct {
	XMLName        xml.Name       `xml:"newses"`
	PaginationInfo paginationInfo `xml:"pagination_info"`
	News           []newsItem     `xml:"news"`
}

// paginationInfo carries the provider count metadata. With page=0,
// totalItems equals the number of returned <news> elements.
type paginationInfo struct {
	TotalItems   int `xml:"totalItems,attr"`
	ItemsPerPage int `xml:"itemsPerPage,attr"`
}

// newsItem is one RSO communication as returned by the list XML.
type newsItem struct {
	ID         string     `xml:"id"`
	Title      string     `xml:"title"`
	Shortcut   string     `xml:"shortcut"`
	Content    string     `xml:"content"`
	RSOAlarm   string     `xml:"rso_alarm"`
	RSOIcon    string     `xml:"rso_icon"`
	ValidFrom  string     `xml:"valid_from"`
	ValidTo    string     `xml:"valid_to"`
	Repetition string     `xml:"repetition"`
	Type       string     `xml:"type"`
	CreatedAt  string     `xml:"created_at"`
	UpdatedAt  string     `xml:"updated_at"`
	Provinces  []province `xml:"provinces>province"`
}

// province is one affected voivodeship inside a news item.
type province struct {
	ID   string `xml:"id,attr"`
	Slug string `xml:"slug,attr"`
	City string `xml:"city,attr"`
	Name string `xml:",chardata"`
}
