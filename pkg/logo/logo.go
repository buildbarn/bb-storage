package logo

import (
	_ "embed" // For "go:embed".
)

//go:embed favicon.svg
var faviconSvg []byte

// FaviconSvg holds the Buildbarn logo and can be used as favicon in web browsers.
var FaviconSvg = faviconSvg
