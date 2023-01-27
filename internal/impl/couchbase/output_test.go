package couchbase_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bxcodec/faker/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/impl/couchbase"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestIntegrationCouchbaseOutput(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)

	bucket := fmt.Sprintf("testing-output-%d", time.Now().Unix())
	require.NoError(t, createBucket(context.Background(), t, servicePort, bucket))
	t.Cleanup(func() {
		require.NoError(t, removeBucket(context.Background(), t, servicePort, bucket))
	})

	// default value
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
`, servicePort, bucket, username, password)

	uid := faker.UUIDHyphenated()
	payload := fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	msg := service.NewMessage([]byte(payload)) // by default use message content
	msg.MetaSetMut("id", uid)                  // by default use meta id for object key.
	err := getOutput(t, config).WriteBatch(context.Background(), service.MessageBatch{
		msg,
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)

	// use processor to validate content:
	testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
}

func getOutput(tb testing.TB, config string) *couchbase.Output {
	tb.Helper()

	confSpec := couchbase.OutputConfig()
	env := service.NewEnvironment()

	oConf, err := confSpec.ParseYAML(config, env)
	require.NoError(tb, err)
	out, err := couchbase.NewOutput(oConf)
	require.NoError(tb, err)
	require.NotNil(tb, out)

	require.NoError(tb, out.Connect(context.Background()))

	return out
}
