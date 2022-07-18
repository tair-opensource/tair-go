package tair_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

const (
	redisIP            = "127.0.0.1"
	redisPort          = "6379"
	redisAddr          = redisIP + ":" + redisPort
	redisSecondaryPort = "6381"
)

const (
	ringShard1Port = "6390"
	ringShard2Port = "6391"
	ringShard3Port = "6392"
)

const (
	sentinelName       = "mymaster"
	sentinelMasterPort = "9123"
	sentinelSlave1Port = "9124"
	sentinelSlave2Port = "9125"
	sentinelPort1      = "9126"
	sentinelPort2      = "9127"
	sentinelPort3      = "9128"
)

var (
	sentinelAddrs = []string{":" + sentinelPort1, ":" + sentinelPort2, ":" + sentinelPort3}

	processes map[string]*redisProcess

	redisMain                                      *redisProcess
	ringShard1, ringShard2, ringShard3             *redisProcess
	sentinelMaster, sentinelSlave1, sentinelSlave2 *redisProcess
	sentinel1, sentinel2, sentinel3                *redisProcess
)

func registerProcess(port string, p *redisProcess) {
	if processes == nil {
		processes = make(map[string]*redisProcess)
	}
	processes[port] = p
}

type redisProcess struct {
	*os.Process
	*redis.Client
}

func redisOptions() *redis.Options {
	return &redis.Options{
		Addr:         redisAddr,
		DB:           15,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,

		MaxRetries: -1,

		PoolSize:           10,
		PoolTimeout:        30 * time.Second,
		IdleTimeout:        time.Minute,
		IdleCheckFrequency: 100 * time.Millisecond,
	}
}

func redisClusterOptions() *redis.ClusterOptions {
	return &redis.ClusterOptions{
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,

		MaxRedirects: 8,

		PoolSize:           10,
		PoolTimeout:        30 * time.Second,
		IdleTimeout:        time.Minute,
		IdleCheckFrequency: 100 * time.Millisecond,
	}
}

var cluster = &clusterScenario{
	ports:     []string{"30001", "30002", "30003", "30004", "30005", "30006"},
	nodeIDs:   make([]string, 6),
	processes: make(map[string]*redisProcess, 6),
	clients:   make(map[string]*tair.TairClient, 6),
}

func eventually(fn func() error, timeout time.Duration) error {
	errCh := make(chan error, 1)
	done := make(chan struct{})
	exit := make(chan struct{})

	go func() {
		for {
			err := fn()
			if err == nil {
				close(done)
				return
			}

			select {
			case errCh <- err:
			default:
			}

			select {
			case <-exit:
				return
			case <-time.After(timeout / 100):
			}
		}
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		close(exit)
		select {
		case err := <-errCh:
			return err
		default:
			return fmt.Errorf("timeout after %s without an error", timeout)
		}
	}
}

func TestGinkgoTairSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "tair-go")
}
