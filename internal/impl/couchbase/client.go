package couchbase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/benthosdev/benthos/v4/internal/impl/couchbase/client"
	"github.com/benthosdev/benthos/v4/public/service"
)

// ErrInvalidTranscoder specified transcoder is not supported.
var ErrInvalidTranscoder = errors.New("invalid transcoder")

type config struct {
	url, bucket, collection string
	timeout                 time.Duration
	opts                    gocb.ClusterOptions
}

type couchbaseClient struct {
	config
	collection *gocb.Collection
	cluster    *gocb.Cluster
}

func generateCouchbaseOpts(username, password, transcoder string, timeout time.Duration) (gocb.ClusterOptions, error) {
	// init couchbase opts
	opts := gocb.ClusterOptions{
		// TODO add opentracing Tracer:
		// TODO add metrics Meter:
	}

	opts.TimeoutsConfig = gocb.TimeoutsConfig{
		ConnectTimeout:    timeout,
		KVTimeout:         timeout,
		KVDurableTimeout:  timeout,
		ViewTimeout:       timeout,
		QueryTimeout:      timeout,
		AnalyticsTimeout:  timeout,
		SearchTimeout:     timeout,
		ManagementTimeout: timeout,
	}

	if username != "" {
		opts.Authenticator = gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	switch client.Transcoder(transcoder) {
	case client.TranscoderJSON:
		opts.Transcoder = gocb.NewJSONTranscoder()
	case client.TranscoderRaw:
		opts.Transcoder = gocb.NewRawBinaryTranscoder()
	case client.TranscoderRawJSON:
		opts.Transcoder = gocb.NewRawJSONTranscoder()
	case client.TranscoderRawString:
		opts.Transcoder = gocb.NewRawStringTranscoder()
	case client.TranscoderLegacy:
		opts.Transcoder = gocb.NewLegacyTranscoder()
	default:
		return opts, fmt.Errorf("%w: %s", ErrInvalidTranscoder, transcoder)
	}

	return opts, nil
}

func parseOptsConf(conf *service.ParsedConfig) (username, password, transcoder string, timeout time.Duration, err error) {
	timeout, err = conf.FieldDuration("timeout")
	if err != nil {
		return
	}

	transcoder, err = conf.FieldString("transcoder")
	if err != nil {
		return
	}

	if conf.Contains("username") {
		username, err = conf.FieldString("username")
		if err != nil {
			return
		}
		password, err = conf.FieldString("password")
		if err != nil {
			return
		}
	}

	return
}

func parseClientConf(conf *service.ParsedConfig) (cfg config, err error) {
	// retrieve params
	cfg.url, err = conf.FieldString("url")
	if err != nil {
		return
	}

	cfg.bucket, err = conf.FieldString("bucket")
	if err != nil {
		return
	}

	if conf.Contains("collection") {
		cfg.collection, err = conf.FieldString("collection")
		if err != nil {
			return
		}
	}

	username, password, transcoder, timeout, err := parseOptsConf(conf)
	if err != nil {
		return
	}
	cfg.timeout = timeout

	cfg.opts, err = generateCouchbaseOpts(username, password, transcoder, timeout)

	return
}

func (clt *couchbaseClient) Connect(ctx context.Context) (err error) {
	if clt.cluster != nil {
		return nil // already connected
	}

	clt.cluster, err = gocb.Connect(clt.url, clt.opts)
	if err != nil {
		return err
	}

	// check that we can do query
	err = clt.cluster.Bucket(clt.bucket).WaitUntilReady(clt.timeout, nil)
	if err != nil {
		return err
	}

	// retrieve collection
	if clt.config.collection != "" {
		clt.collection = clt.cluster.Bucket(clt.bucket).Collection(clt.config.collection)
	} else {
		clt.collection = clt.cluster.Bucket(clt.bucket).DefaultCollection()
	}

	return
}

func (clt *couchbaseClient) Close(ctx context.Context) error {
	err := clt.cluster.Close(&gocb.ClusterCloseOptions{})
	clt.cluster, clt.collection = nil, nil

	return err
}
