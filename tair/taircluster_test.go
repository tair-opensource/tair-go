package tair_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
)

var (
	ctx      = context.Background()
	testHost = "127.0.0.1"
)

type clusterScenario struct {
	ports     []string
	nodeIDs   []string
	processes map[string]*redisProcess
	clients   map[string]*tair.TairClient
}

func (s *clusterScenario) addrs() []string {
	addrs := make([]string, len(s.ports))
	for i, port := range s.ports {
		addrs[i] = net.JoinHostPort(testHost, port)
	}
	return addrs
}

func (s *clusterScenario) newClusterClientUnstable(opt *redis.ClusterOptions) *tair.TairClusterClient {
	opt.Addrs = s.addrs()
	options := &tair.TairClusterOptions{ClusterOptions: opt}
	return tair.NewTairClusterClient(options)
}

func (s *clusterScenario) newClusterClient(
	ctx context.Context, opt *redis.ClusterOptions,
) *tair.TairClusterClient {
	client := s.newClusterClientUnstable(opt)

	err := eventually(func() error {
		if opt.ClusterSlots != nil {
			return nil
		}
		return nil
	}, 30*time.Second)
	if err != nil {
		panic(err)
	}

	return client
}

type TairClusterTestSuite struct {
	suite.Suite
	tairClient *tair.TairClusterClient
}

func (suite *TairClusterTestSuite) SetupTest() {
	suite.tairClient = cluster.newClusterClient(ctx, redisClusterOptions())
	err := suite.tairClient.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		return master.FlushDB(ctx).Err()
	})
	assert.NoError(suite.T(), err)
}

func (suite *TairClusterTestSuite) TearDownTest() {
	_ = suite.tairClient.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		return master.FlushDB(ctx).Err()
	})
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairClusterTestSuite) TestClusterCas() {
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

func (suite *TairClusterTestSuite) TestClusterCasArgs() {
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

func (suite *TairClusterTestSuite) TestClusterExHSet() {
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

func (suite *TairClusterTestSuite) TestClusterExHset() {
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

func (suite *TairClusterTestSuite) TestClusterExZAdd() {
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

func (suite *TairZsetTestSuite) TestClusterExZAddParams() {
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

func TestTairClusterTestSuite(t *testing.T) {
	suite.Run(t, new(TairClusterTestSuite))
}
