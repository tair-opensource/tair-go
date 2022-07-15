package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type TairClient struct {
	*redis.Client
	tairCmdable
	ctx context.Context
}

func NewTairClient(opt *redis.Options) *TairClient {
	c := TairClient{Client: redis.NewClient(opt)}
	c.tairCmdable = c.Process
	return &c
}

func (t *TairClient) TairPipeline() TairPipeline {

	pipe := TairPipeline{
		Pipeline: t.Client.Pipeline().(*redis.Pipeline),
	}
	pipe.init()
	return pipe
}

func (t *TairClient) TairPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	return t.Client.Pipeline().Pipelined(ctx, fn)
}
