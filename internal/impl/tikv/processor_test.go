package tikv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/impl/tikv"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestProcessorConfigLinting(t *testing.T) {
	configTests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "get content not required",
			config: `
tikv:
  address: 
  - 'url'
  id: '${! json("id") }'
  operation: 'get'
`,
		},
		{
			name: "delete content not required",
			config: `
tikv:
  address: 
  - 'url'
  id: '${! json("id") }'
  operation: 'delete'
`,
		},
		{
			name: "missing put content",
			config: `
tikv:
  address: 
  - 'url'
  id: '${! json("id") }'
  operation: 'put'
`,
			errContains: `content must be set for put operations.`,
		},
		{
			name: "put with content",
			config: `
tikv:
  address: 
  - 'url'
  id: '${! json("id") }'
  content: 'root = this'
  operation: 'put'
`,
		},
	}

	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			strm := env.NewStreamBuilder()
			err := strm.AddProcessorYAML(test.config)
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestIntegrationTiKVProcessor(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireTiKV(t)

	uid := faker.UUIDHyphenated()
	payload := fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())

	t.Run("Put", func(t *testing.T) {
		testTiKVProcessorPut(uid, payload, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testTiKVProcessorGet(uid, payload, servicePort, t)
	})
	t.Run("Delete", func(t *testing.T) {
		testTiKVProcessorDelete(uid, servicePort, t)
	})
	t.Run("GetMissing", func(t *testing.T) {
		testTiKVProcessorGetMissing(uid, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Upsert", func(t *testing.T) {
		testTiKVProcessorPut(uid, payload, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testTiKVProcessorGet(uid, payload, servicePort, t)
	})
}
func getProc(tb testing.TB, config string) *tikv.Processor {
	tb.Helper()

	confSpec := tikv.ProcessorConfig()
	env := service.NewEnvironment()

	pConf, err := confSpec.ParseYAML(config, env)
	require.NoError(tb, err)
	proc, err := tikv.NewProcessor(pConf, service.MockResources())
	require.NoError(tb, err)
	require.NotNil(tb, proc)

	return proc
}

func testTiKVProcessorGet(uid, payload, port string, t *testing.T) {
	config := fmt.Sprintf(`
address:
- 'localhost:%s'
id: '${! content() }'
operation: 'get'
`, port)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// message should contain expected payload.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testTiKVProcessorPut(uid, payload, port string, t *testing.T) {
	config := fmt.Sprintf(`
address:
- 'localhost:%s'
id: '${! json("id") }'
content: 'root = this'
operation: 'put'
`, port)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testTiKVProcessorDelete(uid, port string, t *testing.T) {
	config := fmt.Sprintf(`
address:
- 'localhost:%s'
id: '${! content() }'
operation: 'delete'
`, port)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, uid, string(dataOut))
}

func testTiKVProcessorGetMissing(uid, port string, t *testing.T) {
	config := fmt.Sprintf(`
address:
- 'localhost:%s'
id: '${! content() }'
operation: 'get'
`, port)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// message should contain an error.
	assert.Error(t, msgOut[0][0].GetError())

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, uid, string(dataOut))
}
