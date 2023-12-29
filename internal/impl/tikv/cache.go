package tikv

import (
	"context"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"

	"github.com/benthosdev/benthos/v4/internal/impl/tikv/client"
	"github.com/benthosdev/benthos/v4/public/service"
)

// CacheConfig export couchbase Cache specification.
func CacheConfig() *service.ConfigSpec {
	return client.NewConfigSpec().
		// TODO Stable().
		Version("4.25.0").
		Summary(`Use a TiKV instance as a cache.`).
		Field(service.NewDurationField("default_ttl").
			Description("An optional default TTL to set for items, calculated from the moment the item is cached.").
			Optional().
			Advanced())
}

func init() {
	err := service.RegisterCache("tikv", CacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return NewCache(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// Cache stores or retrieves data from couchbase to be used as a cache
type Cache struct {
	*tikvClient

	ttl *time.Duration
}

// NewCache returns a Couchbase cache.
func NewCache(conf *service.ParsedConfig, mgr *service.Resources) (*Cache, error) {
	cl, err := getClient(context.TODO(), conf, mgr)
	if err != nil {
		return nil, err
	}

	var ttl *time.Duration
	if conf.Contains("default_ttl") {
		ttlTmp, err := conf.FieldDuration("default_ttl")
		if err != nil {
			return nil, err
		}
		ttl = &ttlTmp
	}

	return &Cache{
		tikvClient: cl,
		ttl:        ttl,
	}, nil
}

// Get retrieve from cache.
func (c *Cache) Get(ctx context.Context, key string) (data []byte, err error) {
	out, err := c.client.Get(ctx, []byte(key))
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, service.ErrKeyNotFound
		}
		return nil, err
	}
	if out == nil {
		return nil, service.ErrKeyNotFound
	}

	return out, nil
}

// Set update cache.
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if ttl == nil {
		ttl = c.ttl // load default ttl
	}

	if ttl == nil {
		return c.client.Put(ctx, []byte(key), value)
	}

	return c.client.PutWithTTL(ctx, []byte(key), value, uint64(*ttl))
}

// Add insert into cache.
func (c *Cache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	val, err := c.client.Get(ctx, []byte(key))
	if err != nil && !tikverr.IsErrNotFound(err) {
		return err
	}
	if err == nil && val != nil {
		return service.ErrKeyAlreadyExists
	}

	return c.Set(ctx, key, value, ttl)
}

// Delete remove from cache.
func (c *Cache) Delete(ctx context.Context, key string) error {
	return c.client.Delete(ctx, []byte(key))
}
