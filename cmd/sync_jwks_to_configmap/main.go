package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	http_client "github.com/buildbarn/bb-storage/pkg/http/client"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/sync_jwks_to_configmap"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1_apply "k8s.io/client-go/applyconfigurations/core/v1"
	metav1_apply "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// sync_jwks_to_configmap: Download a JSON Web Key Set (JWKS) via HTTP
// and write it into a Kubernetes ConfigMap.
//
// The JWT "Authorization" header parser that can be configured through
// pkg/proto/configuration/jwt/jwt.proto supports validation of
// signatures using public keys stored in a JWKS. It's a common pattern
// to serve these via HTTP, however we don't provide direct support for
// this. This is intentional, for a couple of reasons:
//
// - If the HTTP server is unavailable, the process wouldn't be able to
//   launch properly.
//
// - For large build clusters having many storage nodes, such an
//   approach would lead to an unnecessary number of requests against
//   the HTTP server.
//
// If downloading a JWKS from a remote server is desired, it is possible
// to run this process as a cron job, which writes the JWKS into a
// ConfigMap. The JWKS can then be accessed by creating a volume mount
// for the ConfigMap.

func main() {
	program.RunMain(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
		if len(os.Args) != 2 {
			return status.Error(codes.InvalidArgument, "Usage: sync_jwks_to_configmap sync_jwks_to_configmap.jsonnet")
		}
		var configuration sync_jwks_to_configmap.ApplicationConfiguration
		if err := util.UnmarshalConfigurationFromFile(os.Args[1], &configuration); err != nil {
			return util.StatusWrapf(err, "Failed to read configuration from %s", os.Args[1])
		}

		// Send a HTTP request to fetch the JWKS.
		roundTripper, err := http_client.NewRoundTripperFromConfiguration(configuration.HttpClient)
		if err != nil {
			return util.StatusWrap(err, "Failed to create HTTP client")
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, configuration.JwksUrl, nil)
		if err != nil {
			return util.StatusWrap(err, "Failed to create HTTP request")
		}
		httpClient := &http.Client{Transport: roundTripper}
		response, err := httpClient.Do(request)
		if err != nil {
			return util.StatusWrap(err, "Failed to perform HTTP request")
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			return status.Errorf(codes.Unavailable, "HTTP request failed with status %#v", response.Status)
		}

		// Ensure that the file that is fetched is a valid JSON
		// object. This ensures that if the server misbehaves
		// (e.g., serves a static HTML file), we don't overwrite
		// any previously downloaded JWKS.
		var jwks map[string]any
		err = json.NewDecoder(response.Body).Decode(&jwks)
		response.Body.Close()
		if err != nil {
			return util.StatusWrap(err, "Failed to read and unmarshal HTTP response")
		}
		marshaledJWKS, err := json.Marshal(jwks)
		if err != nil {
			return util.StatusWrap(err, "Failed to marshal JWKS")
		}

		// Write the JWKS to a specified field in a ConfigMap.
		config, err := rest.InClusterConfig()
		if err != nil {
			return util.StatusWrap(err, "Failed to create Kubernetes client configuration")
		}
		clientSet, err := kubernetes.NewForConfig(config)
		if err != nil {
			return util.StatusWrap(err, "Failed to create Kubernetes client")
		}
		metaKind := "ConfigMap"
		metaAPIVersion := "v1"
		if _, err := clientSet.
			CoreV1().
			ConfigMaps(configuration.ConfigMapNamespace).
			Apply(
				ctx,
				&corev1_apply.ConfigMapApplyConfiguration{
					TypeMetaApplyConfiguration: metav1_apply.TypeMetaApplyConfiguration{
						Kind:       &metaKind,
						APIVersion: &metaAPIVersion,
					},
					ObjectMetaApplyConfiguration: &metav1_apply.ObjectMetaApplyConfiguration{
						Name:      &configuration.ConfigMapName,
						Namespace: &configuration.ConfigMapNamespace,
						Annotations: map[string]string{
							"kubernetes.io/change-cause": fmt.Sprintf("Field %#v updated by sync_jwks_to_configmap", configuration.ConfigMapKey),
						},
					},
					BinaryData: map[string][]byte{
						configuration.ConfigMapKey: marshaledJWKS,
					},
				},
				metav1.ApplyOptions{
					FieldManager: "sync_jwks_to_configmap",
					Force:        true,
				}); err != nil {
			return util.StatusWrap(err, "Failed to apply changes to ConfigMap")
		}
		return nil
	})
}
