package couchbase

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/impl/couchbase/client"
	"github.com/benthosdev/benthos/v4/public/service"
)

// OutputConfig export couchbase output specification.
func OutputConfig() *service.ConfigSpec {
	return client.NewConfigSpec().
		// TODO Stable().
		Version("4.12.0").
		Categories("Services").
		Summary("Store output content in Couchbase.").
		Description("Call upsert for each message by batch").
		Field(service.NewInterpolatedStringField("id").Description("Document id.").
			Default(`${! meta("id").catch(uuid_v4()) }`).Example(`${! json("id") }`).Optional()).
		Field(service.NewBloblangField("content").Description("Document content.").
			Default(`root = this`).Example(`root = content().encode("base64")`).Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of batches to be sending in parallel at any given time.").
			Default(1).Optional()).
		Field(service.NewBatchPolicyField("batching"))
	// Add example
}

func init() {
	err := service.RegisterBatchOutput("couchbase", OutputConfig(),
		func(conf *service.ParsedConfig, _ *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			output, err = NewOutput(conf)
			return
		},
	)
	if err != nil {
		panic(err)
	}
}

// Output wrap a couchbase processor.
type Output struct {
	*Processor
}

// NewOutput returns a Couchbase output (a wrapped bath output).
func NewOutput(conf *service.ParsedConfig) (o *Output, err error) {
	o = &Output{
		Processor: &Processor{
			couchbaseClient: &couchbaseClient{},
			op:              upsert,
		},
	}

	o.Processor.couchbaseClient.config, err = parseClientConf(conf)
	if err != nil {
		return
	}

	if o.id, err = conf.FieldInterpolatedString("id"); err != nil {
		return
	}

	if o.content, err = conf.FieldBloblang("content"); err != nil {
		return
	}

	return
}

// WriteBatch Call upsert for each message by batch.
func (o *Output) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	_, err := o.batch(ctx, msgs, true)

	return err
}
