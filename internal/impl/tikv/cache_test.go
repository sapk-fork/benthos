package tikv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/impl/tikv"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"
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
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(servicePort),
	)
}

func BenchmarkIntegrationCache(b *testing.B) {
	ctx := context.Background()
	servicePort := requireTiKV(b)

	config := fmt.Sprintf(`
address:
- localhost:%s
`, servicePort)

	cache := getCache(b, config)

	b.Run("add", func(b *testing.B) {
		uid := faker.UUIDHyphenated()

		for n := 0; n < b.N; n++ {
			require.NoError(b, cache.Add(ctx, fmt.Sprintf("%s:%d", uid, n), []byte("some data"), nil))
		}
	})

	b.Run("set", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			require.NoError(b, cache.Set(ctx, fmt.Sprintf("key:%d", n%1000), []byte("some new data"), nil))
		}
	})

	b.Run("get", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, err := cache.Get(ctx, fmt.Sprintf("key:%d", n%1000))
			require.NoError(b, err)
		}
	})

	b.Run("delete", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			require.NoError(b, cache.Delete(ctx, fmt.Sprintf("key:%d", n%1000)))
		}
	})
}

func getCache(tb testing.TB, config string) *tikv.Cache {
	tb.Helper()

	confSpec := tikv.CacheConfig()
	env := service.NewEnvironment()

	pConf, err := confSpec.ParseYAML(config, env)
	require.NoError(tb, err)
	proc, err := tikv.NewCache(pConf)
	require.NoError(tb, err)
	require.NotNil(tb, proc)

	return proc
}
