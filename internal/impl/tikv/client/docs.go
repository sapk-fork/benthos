package client

import (
	"github.com/benthosdev/benthos/v4/public/service"
)

// NewConfigSpec constructs a new Couchbase ConfigSpec with common config fields
func NewConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		// TODO Stable().
		Field(service.NewURLListField("address").Description("TiKV address.").Example("127.0.0.1:2379"))
	// TODO HostListField ?
}
