package tair_test

import (
	"github.com/alibaba/tair-go/tair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type TairTsTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairTsTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairTsTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairTsTestSuite) TestEXGAE() {
	a := tair.ExSetArgs{}.New()
	a.Ex(10)
	a.Flags(123)
	suite.tairClient.ExSetArgs(ctx, "exstringkey", "foo", a)
	suite.tairClient.TTL(ctx, "exstringkey")
	result, err := suite.tairClient.ExGae(ctx, "exstringkey", "ex", 20).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result[0], "foo")
	assert.Equal(suite.T(), result[1], int64(1))
	assert.Equal(suite.T(), result[2], int64(123))
}

func TestTairTsTestSuite(t *testing.T) {
	suite.Run(t, new(TairTsTestSuite))
}
