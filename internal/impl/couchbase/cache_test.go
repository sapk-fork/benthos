package couchbase_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/impl/couchbase"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestIntegrationCouchbaseCache(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)

	template := `
cache_resources:
  - label: testcache
    couchbase:
      url: couchbase://localhost:$PORT
      username: $VAR1
      password: $VAR2
      bucket: $ID
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
		integration.CacheTestOptVarOne(username),
		integration.CacheTestOptVarTwo(password),
		integration.CacheTestOptPreTest(func(tb testing.TB, ctx context.Context, testID string, vars *integration.CacheTestConfigVars) {
			require.NoError(tb, createBucket(ctx, tb, servicePort, testID))
			tb.Cleanup(func() {
				require.NoError(tb, removeBucket(ctx, tb, servicePort, testID))
			})
		}),
	)
}

func removeBucket(ctx context.Context, tb testing.TB, port, bucket string) error {
	cluster, err := gocb.Connect(fmt.Sprintf("couchbase://localhost:%v", port), gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return err
	}

	return cluster.Buckets().DropBucket(bucket, &gocb.DropBucketOptions{
		Context: ctx,
	})
}

func createBucket(ctx context.Context, tb testing.TB, port, bucket string) error {
	cluster, err := gocb.Connect(fmt.Sprintf("couchbase://localhost:%v", port), gocb.ClusterOptions{
		TimeoutsConfig: gocb.TimeoutsConfig{
			ConnectTimeout:    time.Minute,
			KVTimeout:         time.Minute,
			KVDurableTimeout:  time.Minute,
			ViewTimeout:       time.Minute,
			QueryTimeout:      time.Minute,
			AnalyticsTimeout:  time.Minute,
			SearchTimeout:     time.Minute,
			ManagementTimeout: time.Minute,
		},
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return fmt.Errorf("gocb.Connect: %w", err)
	}

	err = cluster.Buckets().CreateBucket(gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:       bucket,
			RAMQuotaMB: 100, // smallest value and allow max 10 running bucket with cluster-ramsize 1024 from setup script
			BucketType: gocb.CouchbaseBucketType,
		},
	}, nil)
	if err != nil {
		return fmt.Errorf("CreateBucket: %w", err)
	}

	for i := 0; i < 5; i++ { // try five time
		time.Sleep(time.Second)
		err = cluster.Bucket(bucket).WaitUntilReady(time.Second*10, nil)
		if err == nil {
			break
		}
	}

	return err
}

func BenchmarkIntegrationCache(b *testing.B) {
	ctx := context.Background()
	servicePort := requireCouchbase(b)

	bucket := "bench"
	config := fmt.Sprintf(`
couchbase:
  url: couchbase://localhost:%s
  bucket: %s
  username: %s
  password: %s
`, servicePort, bucket, username, password)

	require.NoError(b, createBucket(ctx, b, servicePort, bucket))
	b.Cleanup(func() {
		require.NoError(b, removeBucket(ctx, b, servicePort, bucket))
	})

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

func getCache(tb testing.TB, config string) *couchbase.Cache {
	tb.Helper()

	confSpec := couchbase.CacheConfig()
	env := service.NewEnvironment()

	pConf, err := confSpec.ParseYAML(config, env)
	require.NoError(tb, err)
	proc, err := couchbase.NewCache(pConf)
	require.NoError(tb, err)
	require.NotNil(tb, proc)

	return proc
}
