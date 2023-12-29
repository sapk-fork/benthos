package tikv_test

import (
	"testing"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationTiKVCache(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireTiKV(t)

	template := `
cache_resources:
  - label: testcache
    tikv:
      address:
        - localhost:$PORT
`

	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		//TODO integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(servicePort),
	)
}
