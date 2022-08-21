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

}

func TestTairTsTestSuite(t *testing.T) {
	suite.Run(t, new(TairTsTestSuite))
}
