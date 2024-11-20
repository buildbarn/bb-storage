package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/auth"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/jmespath/go-jmespath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// AuthorizerFactory can be used to construct authorizers based on options
// specified in a configuration message.
type AuthorizerFactory interface {
	// NewAuthorizerFromConfiguration constructs an authorizer based on
	// options specified in a configuration message.
	NewAuthorizerFromConfiguration(configuration *pb.AuthorizerConfiguration) (auth.Authorizer, error)
}

// DefaultAuthorizerFactory constructs deduplicated authorizers based on
// options specified in configuration messages.
var DefaultAuthorizerFactory = NewDeduplicatingAuthorizerFactory(BaseAuthorizerFactory{})

// BaseAuthorizerFactory can be used to construct authorizers based on
// options specified in a configuration message.
type BaseAuthorizerFactory struct{}

// NewAuthorizerFromConfiguration constructs an authorizer based on
// options specified in a configuration message.
func (f BaseAuthorizerFactory) NewAuthorizerFromConfiguration(config *pb.AuthorizerConfiguration) (auth.Authorizer, error) {
	if config == nil {
		return nil, status.Error(codes.InvalidArgument, "Authorizer configuration not specified")
	}
	switch policy := config.Policy.(type) {
	case *pb.AuthorizerConfiguration_Allow:
		return auth.NewStaticAuthorizer(func(in digest.InstanceName) bool { return true }), nil
	case *pb.AuthorizerConfiguration_Deny:
		return auth.NewStaticAuthorizer(func(in digest.InstanceName) bool { return false }), nil
	case *pb.AuthorizerConfiguration_InstanceNamePrefix:
		trie := digest.NewInstanceNameTrie()
		for _, i := range policy.InstanceNamePrefix.AllowedInstanceNamePrefixes {
			instanceNamePrefix, err := digest.NewInstanceName(i)
			if err != nil {
				return nil, err
			}
			trie.Set(instanceNamePrefix, 0)
		}
		return auth.NewStaticAuthorizer(trie.ContainsPrefix), nil
	case *pb.AuthorizerConfiguration_JmespathExpression:
		expression, err := jmespath.Compile(policy.JmespathExpression)
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to compile JMESPath expression")
		}
		return auth.NewJMESPathExpressionAuthorizer(expression), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Unknown authorizer configuration")
	}
}

type deduplicatingAuthorizerFactory struct {
	base AuthorizerFactory
	// Keys are protojson-encoded pb.AuthorizerConfigurations
	known map[string]auth.Authorizer
}

// NewDeduplicatingAuthorizerFactory creates a new AuthorizerFactory
// which returns the same Authorizer for identical configurations,
// which may allow for things like sharing caches.
func NewDeduplicatingAuthorizerFactory(base AuthorizerFactory) AuthorizerFactory {
	return &deduplicatingAuthorizerFactory{
		base:  base,
		known: make(map[string]auth.Authorizer),
	}
}

// NewAuthorizerFromConfiguration creates an Authorizer based on the passed configuration.
func (af *deduplicatingAuthorizerFactory) NewAuthorizerFromConfiguration(config *pb.AuthorizerConfiguration) (auth.Authorizer, error) {
	keyBytes, err := protojson.Marshal(config)
	key := string(keyBytes)
	if err != nil {
		return nil, err
	}
	if _, ok := af.known[key]; !ok {
		a, err := af.base.NewAuthorizerFromConfiguration(config)
		if err != nil {
			return nil, err
		}
		af.known[key] = a
	}
	return af.known[key], nil
}
