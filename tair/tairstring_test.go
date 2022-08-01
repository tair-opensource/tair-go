package tair_test

import (
	"github.com/alibaba/tair-go/tair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TairStringTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairStringTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairStringTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairStringTestSuite) TestCas() {
	suite.tairClient.Set(ctx, "k1", "v1", 0)
	n, err := suite.tairClient.Cas(ctx, "k1", "v2", "v3").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, int64(0))

	n, err = suite.tairClient.Cas(ctx, "k1", "v1", "v3").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), n, int64(1))

	res, err := suite.tairClient.Get(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, "v3")
}

func (suite *TairStringTestSuite) TestCasArgs() {
	suite.tairClient.Set(ctx, "foo", "bzz", 0)
	suite.tairClient.CasArgs(ctx, "foo", "bzz", "too", tair.CasArgs{}.New().Ex(1))

	result, err := suite.tairClient.Get(ctx, "foo").Result()
	assert.Equal(suite.T(), result, "too")
	assert.NoError(suite.T(), err)
	time.Sleep(time.Duration(2) * time.Second)

	result1, err1 := suite.tairClient.Get(ctx, "foo").Result()
	assert.Error(suite.T(), err1)
	assert.Equal(suite.T(), result1, "")
}

func (suite *TairStringTestSuite) TestCad() {
	suite.tairClient.Set(ctx, "foo", "bar", 0)
	res, err := suite.tairClient.Cad(ctx, "foo", "bzz").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(0))

	res1, err1 := suite.tairClient.Cad(ctx, "foo", "bar").Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), res1, int64(1))
}

func (suite *TairStringTestSuite) TestExSetArgs() {
	result2, err2 := suite.tairClient.ExSetArgs(ctx, "foo", "bar", tair.ExSetArgs{}.New().Xx()).Result()
	assert.Error(suite.T(), err2)
	assert.Equal(suite.T(), result2, "")

	result3, err3 := suite.tairClient.ExSetArgs(ctx, "foo", "bar", tair.ExSetArgs{}.New().Nx()).Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, "OK")
}

func (suite *TairStringTestSuite) TestExGet() {
	result2, err2 := suite.tairClient.ExSetArgs(ctx, "foo", "bar", tair.ExSetArgs{}.New().Abs(100)).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, "OK")

	result4, err4 := suite.tairClient.ExGet(ctx, "foo").Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), result4[0], "bar")
	assert.Equal(suite.T(), result4[1], int64(100))
}

func (suite *TairStringTestSuite) TestExGetWithFlags() {
	a := tair.ExSetArgs{}.New()
	a.Abs(88)
	a.Flags(99)
	exSetRes, err := suite.tairClient.ExSetArgs(ctx, "k", "v", a).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), exSetRes, "OK")

	res, err := suite.tairClient.ExGetWithFlags(ctx, "k").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res[0], "v")
	assert.Equal(suite.T(), res[1], int64(88))
	assert.Equal(suite.T(), res[2], int64(99))
}

func (suite *TairStringTestSuite) TestExIncrBy() {
	result, err := suite.tairClient.ExIncrBy(ctx, "foo", 100).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(100))

	a := tair.ExIncrByArgs{}.New()
	a.Max(150)
	_, err1 := suite.tairClient.ExIncrByArgs(ctx, "foo", 100, a).Result()
	assert.Error(suite.T(), err1)
}

func (suite *TairStringTestSuite) TestExIncrByArgs() {
	result, err := suite.tairClient.ExIncrBy(ctx, "foo", 100).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(100))

	a := tair.ExIncrByArgs{}.New()
	a.Max(300)
	res1, err1 := suite.tairClient.ExIncrByArgs(ctx, "foo", 100, a).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), res1, int64(200))
}

func (suite *TairStringTestSuite) TestExIncrByFloat() {
	suite.tairClient.ExSet(ctx, "foo", 100)
	result, err := suite.tairClient.ExIncrByFloat(ctx, "foo", 10.123).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, 110.123)
}

func (suite *TairStringTestSuite) TestExCas() {
	suite.tairClient.ExSet(ctx, "foo", "bar")
	res2, err2 := suite.tairClient.ExCas(ctx, "foo", "bzz", 1).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), res2[0], "OK")
	assert.Equal(suite.T(), res2[1], "")
	assert.Equal(suite.T(), res2[2], int64(2))

	res3, err3 := suite.tairClient.ExCas(ctx, "foo", "bee", 1).Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), res3[0], "CAS_FAILED")
	assert.Equal(suite.T(), res3[1], "bzz")
	assert.Equal(suite.T(), res3[2], int64(2))
}

func (suite *TairStringTestSuite) TestExCad() {
	suite.tairClient.ExSet(ctx, "foo", "bar")
	result, err := suite.tairClient.ExCad(ctx, "foo", 0).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(0))

	result1, err1 := suite.tairClient.ExCad(ctx, "foo", 1).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, int64(1))
}

func (suite *TairStringTestSuite) TestEXAPPEND() {
	result, err := suite.tairClient.ExAppend(ctx, "exstringkey ", "foo", "nx", "ver", 99).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(1))
}

func (suite *TairStringTestSuite) TestEXPREPEND() {
	result, err := suite.tairClient.ExPreAppend(ctx, "exstringkey ", "foo", "nx", "ver", 99).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(1))
}

func (suite *TairStringTestSuite) TestEXGAE() {
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

func TestTairStringTestSuite(t *testing.T) {
	suite.Run(t, new(TairStringTestSuite))
}
