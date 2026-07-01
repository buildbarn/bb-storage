package logo

import (
	_ "embed" // For "go:embed".
	"encoding/base64"
	"html/template"
)

//go:embed favicon.svg
var faviconSvg []byte

// FaviconSvg holds the Buildbarn logo and can be used as favicon in web browsers.
var FaviconSvg = faviconSvg

// EmbeddedFaviconURL has encoded FaviconSvg into a 'data:' URL,
// to be embedded in a web site.
//
// Example:
//
//	<link href="{{EmbeddedFaviconURL}}" rel="icon">
var EmbeddedFaviconURL = template.URL("data:image/svg+xml;base64," + base64.StdEncoding.EncodeToString(FaviconSvg))
