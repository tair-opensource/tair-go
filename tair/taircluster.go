package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type TairClusterClient struct {
	*redis.ClusterClient
	tairCmdable
	ctx context.Context
}

type TairClusterOptions struct {
	*redis.ClusterOptions
}

func (opt *TairClusterOptions) init() {
	//init tair cluster options with necessary setup
}

func NewTairClusterClient(opt *TairClusterOptions) *TairClusterClient {
	opt.init()
	tc := &TairClusterClient{
		ClusterClient: redis.NewClusterClient(opt.ClusterOptions),
	}
	tc.tairCmdable = tc.Process
	return tc
}
