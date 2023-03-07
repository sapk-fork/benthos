package snowflake

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

type MockStream struct {
	Rows      []any
	CallCount int
}

func (db *MockStream) InsertRows(ctx context.Context, rows []any) error {
	db.Rows = append(db.Rows, rows...)
	db.CallCount++

	return nil
}

func (db *MockStream) Close(ctx context.Context) error { return nil }

func TestSnowflakeStreamOutput(t *testing.T) {
	type testCase struct {
		name                      string
		privateKeyPath            string
		privateKeyPassphrase      string
		requestID                 string
		channel                   string
		snowflakeHTTPResponseCode int
		snowflakeResponseCode     string
		wantStreamQuery           string
		wantStreamQueriesCount    int
		wantStreamPayload         string
		wantStreamJWT             string
		errConfigContains         string
		errContains               string
	}
	getSnowflakeStreamWriter := func(t *testing.T, tc testCase) (*snowflakeStreamWriter, error) {
		t.Helper()

		outputConfig := `
account: benthos
region: east-us-2
cloud: azure
user: foobar
private_key_file: ` + tc.privateKeyPath + `
private_key_pass: ` + tc.privateKeyPassphrase + `
role: test_role
database: test_db
schema: test_schema
table: test_table
request_id: '` + tc.requestID + `'
channel: '` + tc.channel + `'
`

		spec := snowflakeStreamOutputConfig()
		env := service.NewEnvironment()
		conf, err := spec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		return newSnowflakeStreamWriterFromConfig(conf, service.MockResources())
	}

	tests := []testCase{
		{
			name:           "executes snowflake query with plaintext SSH key",
			privateKeyPath: "resources/ssh_keys/snowflake_rsa_key.pem",
			channel:        "test_channel",
		},
		{
			name:                 "executes snowflake query with encrypted SSH key",
			privateKeyPath:       "resources/ssh_keys/snowflake_rsa_key.p8",
			privateKeyPassphrase: "test123",
			channel:              "test_channel",
		},
		{
			name:              "fails to read missing SSH key",
			privateKeyPath:    "resources/ssh_keys/missing_key.pem",
			channel:           "test_channel",
			errConfigContains: "failed to read private key resources/ssh_keys/missing_key.pem: open resources/ssh_keys/missing_key.pem: no such file or directory",
		},
		{
			name:              "fails to read encrypted SSH key without passphrase",
			privateKeyPath:    "resources/ssh_keys/snowflake_rsa_key.p8",
			channel:           "test_channel",
			errConfigContains: "failed to read private key: private key requires a passphrase, but private_key_passphrase was not supplied",
		},
		/*
			{
				name:                      "executes snowflake query and calls Snowpipe",
				privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
				stage:                     "@test_stage",
				snowpipe:                  "test_pipe",
				compression:               "NONE",
				snowflakeHTTPResponseCode: http.StatusOK,
				snowflakeResponseCode:     "SUCCESS",
				wantPUTQuery:              "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
				wantPUTQueriesCount:       1,
				wantSnowpipeQuery:         "/v1/data/pipes/test_db.test_schema.test_pipe/insertFiles?requestId=" + dummyUUID,
				wantSnowpipeQueriesCount:  1,
				wantSnowpipePayload:       `{"files":[{"path":"foo/bar/baz/` + dummyUUID + `.json"}]}`,
				wantSnowpipeJWT:           "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi02MjEzNTU5Njc0MCwiaWF0IjotNjIxMzU1OTY4MDAsImlzcyI6IkJFTlRIT1MuRk9PQkFSLlNIQTI1Njprc3dSSG9uZmU0QllXQWtReUlBUDVzY2w5OUxRQ0U2S1Irc0J4VEVoenBFPSIsInN1YiI6IkJFTlRIT1MuRk9PQkFSIn0.ABldbfDem53G-EDMoQaY7VVA2RXPryvXFcY0Hqogu_-qjT3qcJEY1aM1B9SqATkeFDNiagOXPl218dUc-Hes4WTbWnoXq8EUlMLjbg3_9qrlp6p-6SzUbX88lpkuYPXD3UiDBhLXsQso5ciufev2IFX5oCt-Oxg9GbI4uIveey_k8dv3S2a942RQbB6ffCj3Stca31oz2F_IPaF2xDmwVsBig_C9NoHToQFVAfVbPIV1hMDIc7zutuLqXQWZPfT6K0PPc15ZMutQQ0tEYCboDanx3tXe9ub_gLfyGaHwuDUXBk3EN3UkZ8rmgasCk_VnFZ_Xk6tnaZfdIrGKRZ5dsA",
			},
			{
				name:                      "gets error code from Snowpipe",
				privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
				stage:                     "@test_stage",
				snowpipe:                  "test_pipe",
				compression:               "NONE",
				snowflakeHTTPResponseCode: http.StatusOK,
				snowflakeResponseCode:     "FAILURE",
				errContains:               "received unexpected Snowpipe response code: FAILURE",
			},
			{
				name:                      "gets http error from Snowpipe",
				privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
				stage:                     "@test_stage",
				snowpipe:                  "test_pipe",
				compression:               "NONE",
				snowflakeHTTPResponseCode: http.StatusTeapot,
				errContains:               "received unexpected Snowpipe response status: 418",
			},
			{
				name:                "handles stage interpolation and runs a query for each sub-batch",
				privateKeyPath:      "resources/ssh_keys/snowflake_rsa_key.pem",
				stage:               `@test_stage_${! json("id") }`,
				compression:         "NONE",
				wantPUTQueriesCount: 2,
				wantPUTQuery:        "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage_bar/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
			},
			{
				name:                      "handles Snowpipe interpolation and runs a query for each sub-batch",
				privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
				stage:                     "@test_stage",
				snowpipe:                  `test_pipe_${! json("id") }`,
				compression:               "NONE",
				snowflakeHTTPResponseCode: http.StatusOK,
				snowflakeResponseCode:     "SUCCESS",
				wantPUTQuery:              "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
				wantPUTQueriesCount:       2,
				wantSnowpipeQuery:         "/v1/data/pipes/test_db.test_schema.test_pipe_bar/insertFiles?requestId=" + dummyUUID,
				wantSnowpipeQueriesCount:  2,
				wantSnowpipePayload:       `{"files":[{"path":"foo/bar/baz/` + dummyUUID + `.json"}]}`,
				wantSnowpipeJWT:           "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi02MjEzNTU5Njc0MCwiaWF0IjotNjIxMzU1OTY4MDAsImlzcyI6IkJFTlRIT1MuRk9PQkFSLlNIQTI1Njprc3dSSG9uZmU0QllXQWtReUlBUDVzY2w5OUxRQ0U2S1Irc0J4VEVoenBFPSIsInN1YiI6IkJFTlRIT1MuRk9PQkFSIn0.ABldbfDem53G-EDMoQaY7VVA2RXPryvXFcY0Hqogu_-qjT3qcJEY1aM1B9SqATkeFDNiagOXPl218dUc-Hes4WTbWnoXq8EUlMLjbg3_9qrlp6p-6SzUbX88lpkuYPXD3UiDBhLXsQso5ciufev2IFX5oCt-Oxg9GbI4uIveey_k8dv3S2a942RQbB6ffCj3Stca31oz2F_IPaF2xDmwVsBig_C9NoHToQFVAfVbPIV1hMDIc7zutuLqXQWZPfT6K0PPc15ZMutQQ0tEYCboDanx3tXe9ub_gLfyGaHwuDUXBk3EN3UkZ8rmgasCk_VnFZ_Xk6tnaZfdIrGKRZ5dsA",
			},
			{
				name:                      "handles request_id interpolation and runs a query and makes a single Snowpipe call for the entire batch",
				privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
				stage:                     `@test_stage`,
				snowpipe:                  `test_pipe`,
				requestID:                 `${! "deadbeef" }`,
				compression:               "NONE",
				snowflakeHTTPResponseCode: http.StatusOK,
				snowflakeResponseCode:     "SUCCESS",
				wantPUTQuery:              "PUT file://foo/bar/baz/deadbeef.json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
				wantPUTQueriesCount:       1,
				wantSnowpipeQuery:         "/v1/data/pipes/test_db.test_schema.test_pipe/insertFiles?requestId=deadbeef",
				wantSnowpipeQueriesCount:  1,
				wantSnowpipePayload:       `{"files":[{"path":"foo/bar/baz/deadbeef.json"}]}`,
				wantSnowpipeJWT:           "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi02MjEzNTU5Njc0MCwiaWF0IjotNjIxMzU1OTY4MDAsImlzcyI6IkJFTlRIT1MuRk9PQkFSLlNIQTI1Njprc3dSSG9uZmU0QllXQWtReUlBUDVzY2w5OUxRQ0U2S1Irc0J4VEVoenBFPSIsInN1YiI6IkJFTlRIT1MuRk9PQkFSIn0.ABldbfDem53G-EDMoQaY7VVA2RXPryvXFcY0Hqogu_-qjT3qcJEY1aM1B9SqATkeFDNiagOXPl218dUc-Hes4WTbWnoXq8EUlMLjbg3_9qrlp6p-6SzUbX88lpkuYPXD3UiDBhLXsQso5ciufev2IFX5oCt-Oxg9GbI4uIveey_k8dv3S2a942RQbB6ffCj3Stca31oz2F_IPaF2xDmwVsBig_C9NoHToQFVAfVbPIV1hMDIc7zutuLqXQWZPfT6K0PPc15ZMutQQ0tEYCboDanx3tXe9ub_gLfyGaHwuDUXBk3EN3UkZ8rmgasCk_VnFZ_Xk6tnaZfdIrGKRZ5dsA",
			},
		*/
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := getSnowflakeStreamWriter(t, test)
			if test.errConfigContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errConfigContains)
				return
			}

			s.uuidGenerator = MockUUIDGenerator{}

			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.snowflakeHTTPResponseCode)
				_, _ = w.Write([]byte(`{"ResponseCode": "` + test.snowflakeResponseCode + `"}`))
			}))
			t.Cleanup(testServer.Close)

			mockHTTPClient := MockHTTPClient{
				Host: testServer.Listener.Addr().String(),
			}
			s.httpClient = &mockHTTPClient

			mockStream := MockStream{}
			s.stream = &mockStream

			s.nowFn = func() time.Time { return time.Time{} }

			err = s.WriteBatch(context.Background(), service.MessageBatch{
				service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
				service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
			})
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errContains)
				return
			}

			if test.wantStreamQueriesCount > 0 {
				assert.Equal(t, test.wantStreamQueriesCount, mockHTTPClient.QueriesCount)
				assert.Len(t, mockHTTPClient.JWTs, test.wantStreamQueriesCount)
				for _, jwt := range mockHTTPClient.JWTs {
					assert.Equal(t, test.wantStreamJWT, jwt)
				}
			}
			if test.wantStreamQuery != "" {
				assert.True(t, mockHTTPClient.hasQuery(test.wantStreamQuery))
			}
			if test.wantStreamPayload != "" {
				assert.True(t, mockHTTPClient.hasPayload(test.wantStreamPayload))
			}
		})
	}
}
