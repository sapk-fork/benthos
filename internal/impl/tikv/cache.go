package tikv

import (
	"context"
	"log"
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
	log.Printf("Get(%q)", key)

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
	log.Printf("Set(%q, %q, %s)", key, string(value), ttl)

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
	// TODO ttl is not yet supported b SDK with CompareAndSwap but TiKV support it.
	log.Printf("Add(%q, %q, %s)", key, string(value), ttl)
	prev, succeed, err := c.client.CompareAndSwap(ctx, []byte(key), nil, value)
	log.Printf("Add/result(%q): %v %s %s", key, succeed, string(prev), err)
	if err != nil {
		return err
	}
	if !succeed {
		return service.ErrKeyAlreadyExists
	}

	return nil
}

// Delete remove from cache.
func (c *Cache) Delete(ctx context.Context, key string) error {
	log.Printf("Delete(%q)", key)

	return c.client.Delete(ctx, []byte(key))
}
