package snowflake

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang-jwt/jwt"

	"github.com/benthosdev/benthos/v4/public/service"
)

// ResponseStatusCode snowflake status code in body.
type ResponseStatusCode int64

// WriteMode represents the Snowflake steram write mode.
type WriteMode string

const (
	ResponseStatusCodeSuccess ResponseStatusCode = 0

	WriteModeCloudStorage WriteMode = "CLOUD_STORAGE"
	WriteModeRESTAPI      WriteMode = "REST_API"

	// Endpoints
	EndpointChannelStatus = "/v1/streaming/channels/status/"
	EndpointOpenChannel   = "/v1/streaming/channels/open/"
	EndpointRegisterBlob  = "/v1/streaming/channels/write/blobs/"
)

type ColumnMetadata struct {
	Name         string `json:"name"`
	Collation    string `json:"collation"`
	Type         string `json:"type"`
	LogicalType  string `json:"logical_type"`
	PhysicalType string `json:"physical_type"`
	Precision    int    `json:"precision"`
	Scale        int    `json:"scale"`
	ByteLength   int    `json:"byte_length"`
	Length       int    `json:"length"`
	Nullable     bool   `json:"nullable"`
}

type OpenChannelResponse struct {
	StatusCode      ResponseStatusCode `json:"status_code"`
	Message         string             `json:"message"`
	Database        string             `json:"database"`
	Schema          string             `json:"schema"`
	Table           string             `json:"table"`
	Channel         string             `json:"channel"`
	ClientSequencer int64              `json:"client_sequencer"`
	RowSequencer    int64              `json:"row_sequencer"`
	OffsetToken     string             `json:"offset_token"`
	TableColumns    []ColumnMetadata   `json:"table_columns"`
	EncryptionKey   string             `json:"encryption_key"`
	EncryptionKeyId int64              `json:"encryption_key_id"`
}

type RegisterBlobResponse struct {
	StatusCode  ResponseStatusCode   `json:"status_code"`
	Message     string               `json:"message"`
	BlobsStatus []BlobRegisterStatus `json:"blobs"`
}

type BlobRegisterStatus struct {
	ChunksStatus []ChunkRegisterStatus `json:"chunks"`
}

type ChunkRegisterStatus struct {
	ChannelsStatus []ChannelRegisterStatus `json:"channels"`
	Database       string                  `json:"database"`
	Schema         string                  `json:"schema"`
	Table          string                  `json:"table"`
}

type ChannelRegisterStatus struct {
	StatusCode       ResponseStatusCode `json:"status_code"`
	Channel          string             `json:"channel"`
	ChannelSequencer int64              `json:"client_sequencer"`
}

// TODO add https://github.com/snowflakedb/snowflake-ingest-java/blob/37bd7bcb6c095a8b07d9bfdaacdf24369f5458f0/src/main/java/net/snowflake/ingest/streaming/internal/RegisterBlobResponse.java
// TODO add https://github.com/snowflakedb/snowflake-ingest-java/blob/37bd7bcb6c095a8b07d9bfdaacdf24369f5458f0/src/main/java/net/snowflake/ingest/streaming/internal/ChannelsStatusResponse.java

func snowflakeStreamOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Services").
		//  Version("4.13.0").
		Summary("Sends messages to Snowflake tables via stream API.").
		Field(service.NewStringField("account").Description(`Account name, which is the same as the Account Identifier
		as described [here](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used).
		However, when using an [Account Locator](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier),
		the Account Identifier is formatted as ` + "`<account_locator>.<region_id>.<cloud>`" + ` and this field needs to be
		populated using the ` + "`<account_locator>`" + ` part.
		`)).
		Field(service.NewStringField("region").Description(`Optional region field which needs to be populated when using
		an [Account Locator](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier)
		and it must be set to the ` + "`<region_id>`" + ` part of the Account Identifier
		(` + "`<account_locator>.<region_id>.<cloud>`" + `).
		`).Example("us-west-2").Optional()).
		Field(service.NewStringField("cloud").Description(`Optional cloud platform field which needs to be populated
		when using an [Account Locator](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier)
		and it must be set to the ` + "`<cloud>`" + ` part of the Account Identifier
		(` + "`<account_locator>.<region_id>.<cloud>`" + `).
		`).Example("aws").Example("gcp").Example("azure").Optional()).
		Field(service.NewStringField("user").Description("Username.")).
		Field(service.NewStringField("private_key_file").Description("The path to a file containing the private SSH key.")).
		Field(service.NewStringField("private_key_pass").Description("An optional private SSH key passphrase.").Optional().Secret()).
		Field(service.NewStringField("role").Description("Role.")).
		Field(service.NewStringField("database").Description("Database.")).
		Field(service.NewStringField("schema").Description("Schema.")).
		Field(service.NewStringField("table").Description("Table.")).
		Field(service.NewStringField("channel").Description(`A Channel name.`)).
		// TODO ? Field(service.NewInterpolatedStringField("request_id").Description("Request ID. Will be assigned a random UUID (v4) string if not set or empty.").Optional().Default("")).
		// TODO ? Field(service.NewBoolField("client_session_keep_alive").Description("Enable Snowflake keepalive mechanism to prevent the client session from expiring after 4 hours (error 390114).").Advanced().Default(true)).
		Field(service.NewBatchPolicyField("batching")).
		Field(service.NewIntField("max_in_flight").Description("The maximum number of parallel message batches to have in flight at any given time.").Default(1))
}

func init() {
	err := service.RegisterBatchOutput("snowflake_stream", snowflakeStreamOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
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
			output, err = newSnowflakeStreamWriterFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type streamI interface {
	InsertRows(ctx context.Context, rows []any) error
	Close(ctx context.Context) error
}

type snowflakeStream struct {
	setup  *OpenChannelResponse
	writer *snowflakeStreamWriter // not ideal
	// TODO on error
	// TODO default timezone
}

func (s *snowflakeStream) Close(ctx context.Context) error {
	return nil // seems we don't need to do anything
}

func (s *snowflakeStream) InsertRows(ctx context.Context, rows []any) error {
	/*
		response := new(RegisterBlobResponse)

		err := s.writer.callEndpoint(ctx, EndpointRegisterBlob, rows, response)
		if err != nil {
			return fmt.Errorf("failed to send rows to snowflake: %s", err)
		}
	*/
	return nil
}

type snowflakeStreamWriter struct {
	logger *service.Logger

	account           string
	accountIdentifier string
	user              string
	role              string
	database          string
	schema            string
	table             string
	channel           string

	privateKey           *rsa.PrivateKey
	publicKeyFingerprint string

	connMut       sync.Mutex
	nowFn         func() time.Time
	uuidGenerator uuidGenI
	httpClient    httpClientI
	stream        streamI
}

func newSnowflakeStreamWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*snowflakeStreamWriter, error) {
	s := snowflakeStreamWriter{
		logger:        mgr.Logger(),
		uuidGenerator: uuid.NewGen(),
		httpClient:    http.DefaultClient,
		nowFn:         time.Now,
	}

	var err error

	if s.account, err = conf.FieldString("account"); err != nil {
		return nil, fmt.Errorf("failed to parse account: %s", err)
	}

	s.accountIdentifier = s.account

	if conf.Contains("region") {
		var region string
		if region, err = conf.FieldString("region"); err != nil {
			return nil, fmt.Errorf("failed to parse region: %s", err)
		}
		s.accountIdentifier += "." + region
	}

	if conf.Contains("cloud") {
		var cloud string
		if cloud, err = conf.FieldString("cloud"); err != nil {
			return nil, fmt.Errorf("failed to parse cloud: %s", err)
		}
		s.accountIdentifier += "." + cloud
	}

	if s.user, err = conf.FieldString("user"); err != nil {
		return nil, fmt.Errorf("failed to parse user: %s", err)
	}

	if s.role, err = conf.FieldString("role"); err != nil {
		return nil, fmt.Errorf("failed to parse role: %s", err)
	}

	if s.database, err = conf.FieldString("database"); err != nil {
		return nil, fmt.Errorf("failed to parse database: %s", err)
	}

	if s.schema, err = conf.FieldString("schema"); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %s", err)
	}

	if s.channel, err = conf.FieldString("channel"); err != nil {
		return nil, fmt.Errorf("failed to parse channel: %s", err)
	}

	if s.table, err = conf.FieldString("table"); err != nil {
		return nil, fmt.Errorf("failed to parse table: %s", err)
	}

	var privateKeyFile string
	if privateKeyFile, err = conf.FieldString("private_key_file"); err != nil {
		return nil, fmt.Errorf("failed to parse private_key_file: %s", err)
	}

	var privateKeyPass string
	if conf.Contains("private_key_pass") {
		if privateKeyPass, err = conf.FieldString("private_key_pass"); err != nil {
			return nil, fmt.Errorf("failed to parse private_key_pass: %s", err)
		}
	}

	if s.privateKey, err = getPrivateKey(mgr.FS(), privateKeyFile, privateKeyPass); err != nil {
		return nil, fmt.Errorf("failed to read private key: %s", err)
	}

	if s.publicKeyFingerprint, err = calculatePublicKeyFingerprint(s.privateKey); err != nil {
		return nil, fmt.Errorf("failed to calculate public key fingerprint: %s", err)
	}

	return &s, nil
}

//------------------------------------------------------------------------------

func (s *snowflakeStreamWriter) Connect(ctx context.Context) error {
	uuid, err := s.uuidGenerator.NewV4()
	if err != nil {
		return fmt.Errorf("failed to generate requestID: %s", err)
	}

	payload := map[string]string{
		"request_id": uuid.String(),
		"channel":    s.channel,
		"table":      s.table,
		"database":   s.database,
		"schema":     s.schema,
		"write_mode": string(WriteModeCloudStorage), // seems to be the only used in Java SDK
		"role":       s.role,
	}

	response := new(OpenChannelResponse)

	err = s.callEndpoint(ctx, EndpointOpenChannel, payload, response)
	if err != nil {
		return fmt.Errorf("failed to connect to snowflake: %s", err)
	}
	if response.StatusCode != ResponseStatusCodeSuccess {
		return fmt.Errorf("received unexpected stream response code: %d", response.StatusCode)
	}

	s.logger.Debugf("channel opened: %#v", response)

	// store in  s.stream
	s.stream = &snowflakeStream{
		writer: s,
		setup:  response,
	}

	return nil
}

func (s *snowflakeStreamWriter) getStreamURL(endpoint string) string {
	u := url.URL{
		Scheme: "https",
		Host:   fmt.Sprintf("%s.snowflakecomputing.com", s.accountIdentifier),
		Path:   endpoint,
	}
	return u.String()
}

func (s *snowflakeStreamWriter) callEndpoint(ctx context.Context, endpoint string, payload, response any) error {
	jwtToken, err := s.createJWT()
	if err != nil {
		return fmt.Errorf("failed to create Snowpipe JWT token: %s", err)
	}

	buf := bytes.Buffer{}
	if err := json.NewEncoder(&buf).Encode(payload); err != nil {
		return fmt.Errorf("failed to marshal request body JSON: %s", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.getStreamURL(EndpointOpenChannel), &buf)
	if err != nil {
		return fmt.Errorf("failed to create stream HTTP request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+jwtToken)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute stream HTTP request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received unexpected stream response status: %d", resp.StatusCode)
	}

	if err = json.NewDecoder(resp.Body).Decode(response); err != nil {
		return fmt.Errorf("failed to decode stream HTTP response: %s", err)
	}

	return nil
}

func (s *snowflakeStreamWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	s.connMut.Lock()
	defer s.connMut.Unlock()
	if s.stream == nil {
		return service.ErrNotConnected
	}

	var rows []any
	for _, msg := range batch {
		row, err := msg.AsStructured()
		if err != nil {
			return fmt.Errorf("failed to get message structure: %s", err)
		}
		rows = append(rows, row) // only structured data is allowed but maybe it overkill here
	}

	if len(rows) > 0 {
		return s.stream.InsertRows(ctx, rows)
	}

	return nil
}

func (s *snowflakeStreamWriter) Close(ctx context.Context) error {
	s.connMut.Lock()
	defer s.connMut.Unlock()

	return s.stream.Close(ctx)
}

// createJWT creates a new Snowpipe JWT token
// Inspired from https://stackoverflow.com/questions/63598044/snowpipe-rest-api-returning-always-invalid-jwt-token
func (s *snowflakeStreamWriter) createJWT() (string, error) {
	// Need to use the account without the region segment as described in https://stackoverflow.com/questions/65811588/snowflake-jdbc-driver-throws-net-snowflake-client-jdbc-snowflakesqlexception-jw
	qualifiedUsername := strings.ToUpper(s.account + "." + s.user)
	now := s.nowFn().UTC()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": qualifiedUsername + "." + s.publicKeyFingerprint,
		"sub": qualifiedUsername,
		"iat": now.Unix(),
		"exp": now.Add(defaultJWTTimeout).Unix(),
	})

	return token.SignedString(s.privateKey) // TODO deduplicate this func
}
