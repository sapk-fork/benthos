package tikv

import (
	"context"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/impl/tikv/client"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

var (
	// ErrInvalidOperation specified operation is not supported.
	ErrInvalidOperation = errors.New("invalid operation")
)

// ProcessorConfig export couchbase processor specification.
func ProcessorConfig() *service.ConfigSpec {
	return client.NewConfigSpec().
		// TODO Stable().
		Version("4.25.0").
		Categories("Integration").
		Summary("Performs operations against TiKV for each message with RawKV api, allowing you to store or retrieve data within message payloads.").
		Description("When upserting, documents must have the `content` property set.").
		Field(service.NewInterpolatedStringField("id").Description("Document id.").Example(`${! json("id") }`)).
		Field(service.NewBloblangField("content").Description("Document content.").Optional()).
		Field(service.NewStringAnnotatedEnumField("operation", map[string]string{
			string(client.OperationGet):    "fetch a document.",
			string(client.OperationDelete): "delete a document.",
			string(client.OperationPut):    "creates a new document if it does not exist, if it does exist then it updates it.",
		}).Description("Couchbase operation to perform.").Default(string(client.OperationGet))).
		LintRule(`root = if (this.operation == "put" && !this.exists("content")) { [ "content must be set for put operations." ] }`)
}

func init() {
	err := service.RegisterBatchProcessor("tikv", ProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewProcessor(conf)
		},
	)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// Processor stores or retrieves data from couchbase for each message of a
// batch.
type Processor struct {
	*tikvClient
	id      *service.InterpolatedString
	content *bloblang.Executor
	op      client.Operation
}

// NewProcessor returns a Couchbase processor.
func NewProcessor(conf *service.ParsedConfig) (*Processor, error) {
	cl, err := getClient(context.TODO(), conf)
	if err != nil {
		return nil, err
	}
	p := &Processor{
		tikvClient: cl,
	}

	if p.id, err = conf.FieldInterpolatedString("id"); err != nil {
		return nil, err
	}

	if conf.Contains("content") {
		if p.content, err = conf.FieldBloblang("content"); err != nil {
			return nil, err
		}
	}

	op, err := conf.FieldString("operation")
	if err != nil {
		return nil, err
	}

	p.op = client.Operation(op)

	return p, nil
}

// ProcessBatch applies the processor to a message batch, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Processor) ProcessBatch(ctx context.Context, inBatch service.MessageBatch) ([]service.MessageBatch, error) {
	newMsg := inBatch.Copy()
	keys := make([][]byte, len(inBatch))

	// generate keys
	for index := range newMsg {
		// generate id
		k, err := inBatch.TryInterpolatedBytes(index, p.id)
		if err != nil {
			return nil, fmt.Errorf("id interpolation error: %w", err)
		}
		keys[index] = k
	}

	// execute
	switch p.op {
	case client.OperationGet:
		out, err := p.client.BatchGet(ctx, keys)
		if err != nil {
			return nil, err
		}
		// set results (out is alas filled in case of no error)
		for index, part := range newMsg {
			if len(out[index]) == 0 {
				part.SetError(service.ErrKeyNotFound)
			} else {
				part.SetBytes(out[index])
			}
		}
	case client.OperationDelete:
		err := p.client.BatchDelete(ctx, keys)
		if err != nil {
			return nil, err
		}
	case client.OperationPut:
		// generate values
		values := make([][]byte, len(inBatch))
		for index := range newMsg {
			res, err := inBatch.BloblangQuery(index, p.content)
			if err != nil {
				return nil, err
			}
			content, err := res.AsBytes()
			if err != nil {
				return nil, err
			}

			values[index] = content
		}

		err := p.client.BatchPut(ctx, keys, values)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidOperation, p.op)
	}

	return []service.MessageBatch{newMsg}, nil
}
