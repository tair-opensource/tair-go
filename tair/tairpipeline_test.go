package tair_test

import (
	"context"
	"testing"

	"github.com/alibaba/tair-go/tair"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type PipelineTestSuite struct {
	suite.Suite
	tairClient        *tair.TairClient
	tairClusterClient *tair.TairClusterClient
}

func (suite *PipelineTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())

	suite.tairClusterClient = cluster.newClusterClient(ctx, redisClusterOptions())
	err := suite.tairClusterClient.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		return master.FlushDB(ctx).Err()
	})
	assert.NoError(suite.T(), err)
}

func (suite *PipelineTestSuite) TestTairPipeline() {
	pipe := suite.tairClient.TairPipeline()
	pipe.Set(ctx, "key", "value", 0)
	pipe.Get(ctx, "key")
	cmds, err := pipe.Exec(ctx)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "value", cmds[1].(*redis.StringCmd).Val())
}

func (suite *PipelineTestSuite) TestTairPipelined() {
	cmds, err := suite.tairClient.TairPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "key", "value", 0)
		p.Get(ctx, "key")
		return nil
	})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "value", cmds[1].(*redis.StringCmd).Val())
}

func (suite *PipelineTestSuite) TestTairClusterPipeline() {
	pipe := suite.tairClusterClient.TairPipeline()
	pipe.Set(ctx, "key", "value", 0)
	pipe.Get(ctx, "key")
	cmds, err := pipe.Exec(ctx)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "value", cmds[1].(*redis.StringCmd).Val())
}

func (suite *PipelineTestSuite) TestTairClusterPipelined() {
	cmds, err := suite.tairClusterClient.TairPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "key", "value", 0)
		p.Get(ctx, "key")
		return nil
	})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "value", cmds[1].(*redis.StringCmd).Val())
}

func TestTairPipelineTestSuite(t *testing.T) {
	suite.Run(t, new(PipelineTestSuite))
}
