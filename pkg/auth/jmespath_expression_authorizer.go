package auth

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/jmespath/go-jmespath"
)

type jmespathExpressionAuthorizer struct {
	expression *jmespath.JMESPath
}

// NewJMESPathExpressionAuthorizer creates an Authorizer that evaluates
// a JMESPath expression to make an authorization decision. The JMESpath
// expression is called with a JSON object that includes both the REv2
// instance name and authentication metadata.
func NewJMESPathExpressionAuthorizer(expression *jmespath.JMESPath) Authorizer {
	return &jmespathExpressionAuthorizer{
		expression: expression,
	}
}

func (a *jmespathExpressionAuthorizer) Authorize(ctx context.Context, instanceNames []digest.InstanceName) []error {
	authenticationMetadata := AuthenticationMetadataFromContext(ctx)
	errs := make([]error, 0, len(instanceNames))
	for _, instanceName := range instanceNames {
		if result, err := a.expression.Search(map[string]interface{}{
			"authenticationMetadata": authenticationMetadata.GetRaw(),
			"instanceName":           instanceName.String(),
		}); err == nil && result == true {
			errs = append(errs, nil)
		} else {
			errs = append(errs, errPermissionDenied)
		}
	}
	return errs
}
