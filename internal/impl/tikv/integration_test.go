package tikv_test

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

var (
	port               = ""
	integrationCleanup func() error
	integrationOnce    sync.Once
)

// TestMain cleanup tikv cluster if required by tests.
func TestMain(m *testing.M) {
	code := m.Run()
	if integrationCleanup != nil {
		if err := integrationCleanup(); err != nil {
			panic(err)
		}
	}

	os.Exit(code)
}

func requireTiKV(tb testing.TB) string {
	integrationOnce.Do(func() {
		pool, pd, tikv, err := setupTiKV(tb)
		require.NoError(tb, err)

		port = pd.GetPort("2379/tcp")
		integrationCleanup = func() error {
			return errors.Join(pool.Purge(pd), pool.Purge(tikv))
		}
	})

	return port
}

func setupTiKV(tb testing.TB) (*dockertest.Pool, *dockertest.Resource, *dockertest.Resource, error) {
	tb.Log("setup tikv cluster")

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, nil, err
	}

	pwd, err := os.Getwd()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to get working directory: %s", err)
	}

	ressourcePD, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "pd",
		Repository: "pingcap/pd",
		Tag:        "latest",
		Entrypoint: []string{"/bin/sh"},
		Cmd:        []string{"/entrypoints/pd.sh"},
		Mounts: []string{
			fmt.Sprintf("%s/testdata:/entrypoints", pwd),
		},
		ExtraHosts: []string{"host.docker.internal:host-gateway"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"2379/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "2379",
				},
			},
			"2380/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "2380",
				},
			},
		},
	})
	if err != nil {
		return nil, nil, nil, err
	}

	ressourceTiKV, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:       "tikv",
		Repository: "pingcap/tikv",
		Tag:        "latest",
		Entrypoint: []string{"/bin/sh"},
		Cmd:        []string{"/entrypoints/tikv.sh"},
		Mounts: []string{
			fmt.Sprintf("%s/testdata:/entrypoints", pwd),
		},
		ExtraHosts: []string{"host.docker.internal:host-gateway"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"20160/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "20160",
				},
			},
			"20180/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "20180",
				},
			},
		},
	})
	if err != nil {
		return nil, nil, nil, err
	}

	// Look for readyness
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/v2/members", ressourcePD.GetPort("2379/tcp")))
		if err == nil {
			defer resp.Body.Close()
		}

		return err
	}); err != nil {
		log.Fatalf("Could not connect to tikv: %s", err)
	}

	tb.Log("tikv cluster ready")

	return pool, ressourcePD, ressourceTiKV, nil
}
