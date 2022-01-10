package auth

// AuthenticationMetadata is a key type for context.Context that
// authentication frameworks can use to pass metadata to implementations
// of Authorizer. The value is expected to be JSON marshallable.
type AuthenticationMetadata struct{}
