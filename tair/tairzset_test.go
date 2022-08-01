package tair_test

import (
	"github.com/alibaba/tair-go/tair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TairZsetTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairZsetTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairZsetTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairZsetTestSuite) TestExZAdd() {
	res, err := suite.tairClient.ExZAdd(ctx, "k1", "90.1", "v1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	zRangeRes, err := suite.tairClient.ExZRange(ctx, "k1", 0, -1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), zRangeRes[0], "v1")

	res, err = suite.tairClient.ExZAdd(ctx, "foo", "1", "a").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res, err = suite.tairClient.ExZAdd(ctx, "foo", "10", "b").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res, err = suite.tairClient.ExZAdd(ctx, "foo", "2", "a").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(0))
}

func (suite *TairZsetTestSuite) TestExZAddParams() {
	res, err := suite.tairClient.ExZAddArgs(ctx, "foo", "1", "a", tair.ExZAddArgs{}.New().Xx()).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(0))

	res, err = suite.tairClient.ExZAdd(ctx, "foo", "1", "a").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res, err = suite.tairClient.ExZAddArgs(ctx, "foo", "2", "a", tair.ExZAddArgs{}.New().Nx()).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(0))

	res, err = suite.tairClient.ExZAddManyMemberArgs(ctx, "foo", tair.ExZAddArgs{}.New().Ch(),
		tair.ExZAddMember{Score: "2", Member: "a"}, tair.ExZAddMember{Score: "1", Member: "b"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(2))
}

func (suite *TairZsetTestSuite) TestExZRangeBasic() {
	res, err := suite.tairClient.ExZAddManyMember(ctx, "foo",
		tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
		tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(3))

	ss, err := suite.tairClient.ExZRange(ctx, "foo", 0, 1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ss, []string{"c", "a"})

	ss, err = suite.tairClient.ExZRange(ctx, "foo", 0, -1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ss, []string{"c", "a", "b"})

	ss, err = suite.tairClient.ExZRevRange(ctx, "foo", 0, 1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ss, []string{"b", "a"})
}

func (suite *TairZsetTestSuite) TestExZRangeByLex() {
	res, err := suite.tairClient.ExZAddManyMember(ctx, "foo",
		tair.ExZAddMember{Score: "1", Member: "aa"}, tair.ExZAddMember{Score: "1", Member: "c"},
		tair.ExZAddMember{Score: "1", Member: "bb"}, tair.ExZAddMember{Score: "1", Member: "d"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(4))

	ss, err := suite.tairClient.ExZRangeByLex(ctx, "foo", "(aa", "[c").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ss, []string{"bb", "c"})

	ss, err = suite.tairClient.ExZRangeByLexWithArgs(ctx, "foo", "-", "+", tair.ExZRangeArgs{}.New().Limit(1, 2)).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ss, []string{"bb", "c"})
}

func (suite *TairZsetTestSuite) TestExZRemBasic() {
	res, err := suite.tairClient.ExZAddManyMember(ctx, "foo",
		tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "2", Member: "b"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(2))

	is, err := suite.tairClient.ExZRem(ctx, "foo", "a").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), is, int64(1))
}

func (suite *TairZsetTestSuite) TestExZIncrbyBacis() {
	res, err := suite.tairClient.ExZAddManyMember(ctx, "foo",
		tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "2", Member: "b"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(2))

	ss, err := suite.tairClient.ExZIncrBy(ctx, "foo", "2", "a").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ss, "3")
}

func (suite *TairZsetTestSuite) TestExZrankBasic() {
	res, err := suite.tairClient.ExZAddManyMember(ctx, "foo",
		tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "2", Member: "b"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(2))

	res, err = suite.tairClient.ExZRank(ctx, "foo", "a").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(0))

	res, err = suite.tairClient.ExZRank(ctx, "foo", "b").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	res, err = suite.tairClient.ExZRevRank(ctx, "foo", "a").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))
}

func (suite *TairZsetTestSuite) TestExZRangeWithScorBasic() {
	res, err := suite.tairClient.ExZAddManyMember(ctx, "foo",
		tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
		tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(3))

	ss, err := suite.tairClient.ExZRangeWithScores(ctx, "foo", 0, 1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ss, []string{"c", "0.10000000000000001", "a", "2"})
}

func (suite *TairZsetTestSuite) TestExZcardBasic() {
	res, err := suite.tairClient.ExZAddManyMember(ctx, "foo",
		tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
		tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(3))

	res, err = suite.tairClient.ExZCard(ctx, "foo").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(3))

	ss, err := suite.tairClient.ExZScore(ctx, "foo", "b").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ss, "10")
}

func (suite *TairZsetTestSuite) TestExZCountBasic() {
	res, err := suite.tairClient.ExZAddManyMember(ctx, "foo",
		tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
		tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(3))

	res, err = suite.tairClient.ExZCount(ctx, "foo", "0.01", "2.1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(2))
}

func (suite *TairZsetTestSuite) TestExZRemRangeByRank() {
	res, err := suite.tairClient.ExZAddManyMember(ctx, "foo",
		tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
		tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(3))

	res, err = suite.tairClient.ExZRemRangeByRank(ctx, "foo", 0, 0).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), res, int64(1))

	ss, err := suite.tairClient.ExZRange(ctx, "foo", 0, -1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), ss, []string{"a", "b"})
}

func TestTairZsetTestSuite(t *testing.T) {
	suite.Run(t, new(TairZsetTestSuite))
}
