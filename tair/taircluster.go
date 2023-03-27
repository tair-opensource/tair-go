package tair

import (
	"context"

	"github.com/go-redis/redis/v8"
)

var _ TairCmdable = (*TairClusterClient)(nil)

type TairClusterClient struct {
	*redis.ClusterClient
	tairCmdable
	ctx context.Context
}

type TairClusterOptions struct {
	*redis.ClusterOptions
}

func (opt *TairClusterOptions) init() {
}

func NewTairClusterClient(opt *TairClusterOptions) *TairClusterClient {
	opt.init()
	tc := &TairClusterClient{
		ClusterClient: redis.NewClusterClient(opt.ClusterOptions),
		ctx:           context.Background(),
	}
	tc.tairCmdable = tc.Process
	return tc
}

func (t *TairClusterClient) TairPipeline() TairPipeline {
	pipe := TairPipeline{
		Pipeline: t.ClusterClient.Pipeline().(*redis.Pipeline),
	}
	pipe.init()
	return pipe
}

func (t *TairClusterClient) TairPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return t.ClusterClient.Pipeline().Pipelined(ctx, fn)
}
