package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type TairPipeline struct {
	*redis.Pipeline // receive pipeline method to handle multi cmd
	tairCmdable     // receive tair module command
}

func (p *TairPipeline) init() {
	p.tairCmdable = p.Process
}

func (t *TairClient) Process(ctx context.Context, cmd redis.Cmder) error {
	return t.Client.Process(ctx, cmd)
}
