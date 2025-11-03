package global

import (
	"net/http"

	"github.com/sercand/kuberesolver/v5"
)

type simpleK8sClient struct {
	httpClient *http.Client
	url        string
}

// newSimpleK8sClient creates a Kubernetes API server client for use
// with kuberesolver. The implementation that ships with kuberesolver
// makes strong assumptions about pathnames and environment variables.
func newSimpleK8sClient(httpClient *http.Client, url string) kuberesolver.K8sClient {
	return &simpleK8sClient{
		httpClient: httpClient,
		url:        url,
	}
}

func (simpleK8sClient) GetRequest(url string) (*http.Request, error) {
	return http.NewRequest(http.MethodGet, url, nil)
}

func (kc *simpleK8sClient) Do(req *http.Request) (*http.Response, error) {
	return kc.httpClient.Do(req)
}

func (kc *simpleK8sClient) Host() string {
	return kc.url
}
