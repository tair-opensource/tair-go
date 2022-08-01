package tair_test

import (
	"sort"
	"testing"
	"time"
)

type TairHashTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairHashTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairHashTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairHashTestSuite) TestExHSet() {
	res, err := suite.tairClient.ExHSet(ctx, "k1", "f1", "v1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res, err = suite.tairClient.Exists(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	result, err := suite.tairClient.ExHGet(ctx, "k1", "f1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, "v1")
}

func (suite *TairHashTestSuite) TestExHset() {
	res, err := suite.tairClient.ExHSet(ctx, "k1", "f1", "v1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res, err = suite.tairClient.Exists(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	result, err := suite.tairClient.ExHGet(ctx, "k1", "f1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, "v1")
}

func (suite *TairHashTestSuite) TestExHSetByArgs() {
	a := tair.ExHSetArgs{}.New()
	a.Set = make(map[string]bool)
	a.Xx()
	res, err := suite.tairClient.ExHSetArgs(ctx, "k1", "f1", "v1", a).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(-1))

	res, err = suite.tairClient.Exists(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(0))
}

func (suite *TairHashTestSuite) TestExHSetNx() {
	res, err := suite.tairClient.ExHSetNx(ctx, "k1", "f1", "v1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res, err = suite.tairClient.Exists(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))
}

func (suite *TairHashTestSuite) TestExHMget() {
	a := make(map[string]string)
	a["f1"] = "v1"
	a["f2"] = "v2"
	a["f3"] = "v3"
	res2, err2 := suite.tairClient.ExHMSet(ctx, "k1", a).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), res2, "OK")

	result, err := suite.tairClient.ExHMGet(ctx, "k1", "f1", "f2", "f3").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result[0], "v1")
	assert.Equal(suite.T(), result[1], "v2")
	assert.Equal(suite.T(), result[2], "v3")
}

func (suite *TairHashTestSuite) TestExHMSetWithOpts() {
	b := tair.ExHMSetWithOptsArgs{}.New()
	b.Field("f1")
	b.Value("v1")
	b.SetExp(5)
	b.SetVer(99)
	_, err := suite.tairClient.ExHMSetWithOpts(ctx, "k1", b).Result()
	assert.NoError(suite.T(), err)
}

func (suite *TairHashTestSuite) TestExHPExpire() {
	res, err := suite.tairClient.ExHSet(ctx, "k1", "f1", "v1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))
	suite.tairClient.ExHExpire(ctx, "k1", "f1", 1)
	time.Sleep(time.Duration(2) * time.Second)

	res, err1 := suite.tairClient.Exists(ctx, "k1").Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), res, int64(0))
}

func (suite *TairHashTestSuite) TestExHSetVer() {
	res, err := suite.tairClient.ExHSet(ctx, "k1", "f1", "v1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res1, err := suite.tairClient.ExHSetVer(ctx, "k1", "f1", 10).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res1, true)

	res, err = suite.tairClient.ExHVer(ctx, "k1", "f1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(10))
}

func (suite *TairHashTestSuite) TestExHIncrBy() {
	res, err := suite.tairClient.ExHIncrBy(ctx, "k1", "f1", 1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res, err = suite.tairClient.ExHIncrBy(ctx, "k1", "f1", -1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(0))

	res, err = suite.tairClient.ExHIncrBy(ctx, "k1", "f1", -10).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(-10))
}

func (suite *TairHashTestSuite) TestExHIncrByArgs() {
	//_, err := suite.tairClient.ExHIncrByArgs(ctx, "k1", "f1", 11, tair.ExHIncrArgs{}.New().Min(0).Max(10)).Result()
	//assert.Equal(suite.T(), err.Error(), )
}

func (suite *TairHashTestSuite) TestExHIncrByFloat() {
	res, err := suite.tairClient.ExHIncrByFloat(ctx, "k1", "f1", 1.5).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, "1.5")

	res, err = suite.tairClient.ExHIncrByFloat(ctx, "k1", "f1", -1.5).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, "0")

	res, err = suite.tairClient.ExHIncrByFloat(ctx, "k1", "f1", -10.7).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, "-10.7")
}

func (suite *TairHashTestSuite) TestExHIncrByFloatExpire() {
	res, err := suite.tairClient.ExHIncrByFloatArgs(ctx, "k1", "f1", 5.1, tair.ExHIncrArgs{}.New().Ex(1)).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, "5.1")

	time.Sleep(time.Duration(2) * time.Second)

	res2, err := suite.tairClient.ExHLen(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res2, int64(0))
}

func (suite *TairHashTestSuite) TestExHGetWithVer() {
	res, err := suite.tairClient.ExHSet(ctx, "k1", "f1", "v1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res2, err := suite.tairClient.ExHGetWithVer(ctx, "k1", "f1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res2[0], "v1")
	assert.Equal(suite.T(), res2[1], int64(1))
}

func (suite *TairHashTestSuite) TestExHMGetWithVer() {
	a := make(map[string]string)
	a["f1"] = "v1"
	a["f2"] = "v2"
	a["f3"] = "v3"
	res2, err2 := suite.tairClient.ExHMSet(ctx, "k1", a).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), res2, "OK")

	result, err := suite.tairClient.ExHMGetWithVer(ctx, "k1", "f1", "f2", "f3").Result()
	assert.NoError(suite.T(), err)
	v1 := make([]interface{}, 0)
	v1 = append(v1, "v1", int64(1))
	assert.Equal(suite.T(), result[0], v1)
}

func (suite *TairHashTestSuite) TestExHDelExHLen() {
	a := make(map[string]string)
	a["f1"] = "v1"
	a["f2"] = "v2"
	a["f3"] = "v3"
	res2, err2 := suite.tairClient.ExHMSet(ctx, "k1", a).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), res2, "OK")

	res, err := suite.tairClient.ExHDel(ctx, "k1", "not-exists").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(0))

	res, err = suite.tairClient.ExHLen(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(3))

	res, err = suite.tairClient.ExHDel(ctx, "k1", "f1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res3, err := suite.tairClient.ExHExists(ctx, "k1", "f1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res3, false)

	res, err = suite.tairClient.ExHLen(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(2))

	res, err = suite.tairClient.ExHStrLen(ctx, "k1", "f2").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(2))
}

func (suite *TairHashTestSuite) TestExHKeysExHVals() {
	a := make(map[string]string)
	a["f1"] = "v1"
	a["f2"] = "v2"
	a["f3"] = "v3"
	res2, err2 := suite.tairClient.ExHMSet(ctx, "k1", a).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), res2, "OK")

	res, err := suite.tairClient.ExHKeys(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	sort.Strings(res)
	assert.Equal(suite.T(), res, []string{"f1", "f2", "f3"})

	res, err = suite.tairClient.ExHVals(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	sort.Strings(res)
	assert.Equal(suite.T(), res, []string{"v1", "v2", "v3"})
}

func (suite *TairHashTestSuite) TestExHGetAll() {
	a := make(map[string]string)
	a["f1"] = "v1"
	a["f2"] = "v2"
	a["f3"] = "v3"
	res2, err2 := suite.tairClient.ExHMSet(ctx, "k1", a).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), res2, "OK")

	res, err := suite.tairClient.ExHGetAll(ctx, "k1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})
}

func TestTairHashTestSuite(t *testing.T) {
	suite.Run(t, new(TairHashTestSuite))
}
