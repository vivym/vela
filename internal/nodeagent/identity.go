package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// StaticControllerIdentityResolver is the deployment-time identity map for a
// Node Agent. The map is intentionally explicit; a certificate being signed by
// the trusted CA is not enough to authorize a controller actor.
type StaticControllerIdentityResolver struct {
	identities map[string]ControllerIdentity
}

func NewStaticControllerIdentityResolver(identities map[string]string) (*StaticControllerIdentityResolver, error) {
	if len(identities) == 0 {
		return nil, errors.New("at least one controller identity is required")
	}
	resolved := make(map[string]ControllerIdentity, len(identities))
	for spiffeID, actorIdentity := range identities {
		parsed, err := url.Parse(spiffeID)
		if err != nil || !validSPIFFEID(parsed) || !isControllerSPIFFEID(parsed) {
			return nil, fmt.Errorf("controller SPIFFE identity %q is invalid", spiffeID)
		}
		if !validText(actorIdentity, maxIdentityText) {
			return nil, fmt.Errorf("controller actor identity for %q is invalid", spiffeID)
		}
		if _, duplicate := resolved[spiffeID]; duplicate {
			return nil, fmt.Errorf("controller SPIFFE identity %q is duplicated", spiffeID)
		}
		resolved[spiffeID] = ControllerIdentity{SPIFFEIdentity: spiffeID, ActorIdentity: actorIdentity}
	}
	return &StaticControllerIdentityResolver{identities: resolved}, nil
}

func (resolver *StaticControllerIdentityResolver) ResolveController(
	_ context.Context,
	spiffeID string,
) (ControllerIdentity, error) {
	if resolver == nil {
		return ControllerIdentity{}, errors.New("controller identity resolver is not configured")
	}
	identity, ok := resolver.identities[spiffeID]
	if !ok {
		return ControllerIdentity{}, errors.New("controller SPIFFE identity is not registered")
	}
	return identity, nil
}

func isControllerSPIFFEID(identity *url.URL) bool {
	return identity != nil && identity.Host == "vela.internal" &&
		strings.HasPrefix(identity.Path, "/controller/") &&
		strings.TrimPrefix(identity.Path, "/controller/") != ""
}

var _ ControllerIdentityResolver = (*StaticControllerIdentityResolver)(nil)
