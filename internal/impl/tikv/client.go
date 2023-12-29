package tikv

import (
	"context"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"

	"github.com/benthosdev/benthos/v4/public/service"
)

type tikvClient struct {
	client *rawkv.Client
}

func getClient(ctx context.Context, conf *service.ParsedConfig, mgr *service.Resources) (*tikvClient, error) {
	// retrieve params
	addressList, err := conf.FieldStringList("address")
	if err != nil {
		return nil, err
	}

	client, err := rawkv.NewClient(ctx, addressList, config.DefaultConfig().Security)
	if err != nil {
		return nil, err
	}

	client.SetAtomicForCAS(true)

	return &tikvClient{
		client: client,
	}, nil
}

func (p *tikvClient) Close(ctx context.Context) error {
	return p.client.Close()
}
