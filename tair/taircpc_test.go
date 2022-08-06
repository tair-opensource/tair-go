package tair_test

import (
	"github.com/alibaba/tair-go/tair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var key string = "key" + randStrCpc(20)
var key2 string = "key2" + randStrCpc(20)
var key3 string = "key3" + randStrCpc(20)
var item string = "item" + randStrCpc(20)
var item2 string = "item2" + randStrCpc(20)
var item3 string = "item3" + randStrCpc(20)
var item4 string = "item4" + randStrCpc(20)
var content1 string = "content1" + randStrCpc(20)
var content2 string = "content2" + randStrCpc(20)

var count1 int64 = 100
var count2 int64 = 200
var timestamp int64 = 1000000
var winSize int64 = 6000

func randStrCpc(size int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := []byte(str)
	var result []byte
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100000)))
	for i := 0; i < size; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

type TairCpcTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairCpcTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairCpcTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairCpcTestSuite) TestCpcUpdate() {
	r, e := suite.tairClient.CpcUpdate(ctx, key, item).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")
}

func (suite *TairCpcTestSuite) TestCpcUpdateExpire() {
	args := tair.CpcUpdateArgs{}.New()
	args.SetEx(2)
	r, e := suite.tairClient.CpcUpdateArgs(ctx, key, item, args).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.CpcUpdate(ctx, key, item2).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "OK")

	r2, e2 := suite.tairClient.CpcEstimate(ctx, key).Result()
	assert.NoError(suite.T(), e2)
	assert.InDelta(suite.T(), r2, 2.00, 0.001)

	time.Sleep(2 * time.Second)

	_, e3 := suite.tairClient.CpcEstimate(ctx, key).Result()
	assert.Error(suite.T(), e3)

	r4, e4 := suite.tairClient.CpcUpdate(ctx, key, item).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), r4, "OK")

	args.SetEx(0)
	r5, e5 := suite.tairClient.CpcUpdateArgs(ctx, key, item2, args).Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), r5, "OK")

	time.Sleep(1000)
	r6, e6 := suite.tairClient.CpcEstimate(ctx, key).Result()
	assert.NoError(suite.T(), e6)
	assert.InDelta(suite.T(), r6, 2, 0.001)

	args.SetEx(2)
	r7, e7 := suite.tairClient.CpcUpdateArgs(ctx, key, item2, args).Result()
	assert.NoError(suite.T(), e7)
	assert.Equal(suite.T(), r7, "OK")

	args.SetEx(-1)
	r8, e8 := suite.tairClient.CpcUpdateArgs(ctx, key, item2, args).Result()
	assert.NoError(suite.T(), e8)
	assert.Equal(suite.T(), r8, "OK")

	time.Sleep(2000)
	r9, e9 := suite.tairClient.CpcEstimate(ctx, key).Result()
	assert.NoError(suite.T(), e9)
	assert.InDelta(suite.T(), r9, 2, 0.001)
}

func (suite *TairCpcTestSuite) TestCpcUpdate2Jud() {
	r, e := suite.tairClient.CpcUpdate(ctx, key, item).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.CpcUpdate(ctx, key, item2).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "OK")

	r2, e2 := suite.tairClient.CpcUpdate(ctx, key, item3).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "OK")

	r3, e3 := suite.tairClient.CpcEstimate(ctx, key).Result()
	assert.NoError(suite.T(), e3)
	assert.InDelta(suite.T(), r3, 3.00, 0.001)

	r4, e4 := suite.tairClient.CpcUpdate2Jud(ctx, key, item).Result()
	assert.NoError(suite.T(), e4)
	assert.InDelta(suite.T(), r4.Value(), 3, 0.001)
	assert.InDelta(suite.T(), r4.DiffValue(), 0.0, 0.001)

	r5, e5 := suite.tairClient.CpcUpdate2Jud(ctx, key, item4).Result()
	assert.NoError(suite.T(), e5)
	assert.InDelta(suite.T(), r5.Value(), 4, 0.001)
	assert.InDelta(suite.T(), r5.DiffValue(), 1, 0.001)
}
func (suite *TairCpcTestSuite) TestCpcAccurateEstimation() {
	for i := 0; i < 120; i++ {
		itemTemp := item + strconv.Itoa(i)
		r1, e1 := suite.tairClient.CpcUpdate(ctx, key, itemTemp).Result()
		assert.NoError(suite.T(), e1)
		assert.Equal(suite.T(), r1, "OK")
	}
	r, e := suite.tairClient.CpcEstimate(ctx, key).Result()
	assert.NoError(suite.T(), e)
	assert.InDelta(suite.T(), r, 120, 0.1)
}

func (suite *TairCpcTestSuite) TestCpcUpdate2Est() {
	for i := 0; i < 120; i++ {
		itemTemp := item + strconv.Itoa(i)
		r1, e1 := suite.tairClient.CpcUpdate2Est(ctx, key, itemTemp).Result()
		assert.NoError(suite.T(), e1)
		assert.InDelta(suite.T(), r1, i+1, float64(i)+0.1)
	}
	r, e := suite.tairClient.CpcEstimate(ctx, key).Result()
	assert.NoError(suite.T(), e)
	assert.InDelta(suite.T(), r, 120, 0.1)
}

func (suite *TairCpcTestSuite) TestCpcArrayUpdateAndEstimate() {
	r, e := suite.tairClient.CpcArrayUpdate(ctx, key, 1, item).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e2 := suite.tairClient.CpcArrayUpdate(ctx, key, 1, item2).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r1, "OK")

	r3, e3 := suite.tairClient.CpcArrayUpdate(ctx, key, 3, item).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), r3, "OK")

	r4, e4 := suite.tairClient.CpcArrayUpdate(ctx, key, 5, item).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), r4, "OK")

	r5, e5 := suite.tairClient.CpcArrayEstimate(ctx, key, 1).Result()
	assert.NoError(suite.T(), e5)
	assert.InDelta(suite.T(), r5, 2, 0.001)

	r6, e6 := suite.tairClient.CpcArrayEstimate(ctx, key, 3).Result()
	assert.NoError(suite.T(), e6)
	assert.InDelta(suite.T(), r6, 2, 0.001)

	r7, e7 := suite.tairClient.CpcArrayEstimate(ctx, key, 5).Result()
	assert.NoError(suite.T(), e7)
	assert.InDelta(suite.T(), r7, 2, 0.001)
}

func (suite *TairCpcTestSuite) TestCpcArrayEstimateRange() {
	r, e := suite.tairClient.CpcArrayUpdate(ctx, key, timestamp, item).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.CpcArrayEstimateRange(ctx, key, timestamp-1000, timestamp+1000).Result()
	assert.NoError(suite.T(), e1)
	assert.InDelta(suite.T(), r1[0], 1, 0.1)
}

func (suite *TairCpcTestSuite) TestCpcArrayEstimateRangeMerge() {
	r, e := suite.tairClient.CpcArrayUpdate(ctx, key, timestamp, item).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.CpcArrayEstimateRange(ctx, key, timestamp-1000, timestamp+1000).Result()
	assert.NoError(suite.T(), e1)
	assert.InDelta(suite.T(), r1[0], 1, 0.1)

	r2, e2 := suite.tairClient.CpcArrayEstimateRangeMerge(ctx, key, timestamp, 10000).Result()
	assert.NoError(suite.T(), e2)
	assert.InDelta(suite.T(), r2, 1, 0.1)
}

func (suite *TairCpcTestSuite) TestCpcArrayUpdate2Est() {
	r, e := suite.tairClient.CpcArrayUpdate(ctx, key, timestamp, item).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")
	r1, e1 := suite.tairClient.CpcArrayEstimateRange(ctx, key, timestamp-1000, timestamp+1000).Result()
	assert.NoError(suite.T(), e1)
	assert.InDelta(suite.T(), r1[0], 1, 0.001)
}

func (suite *TairCpcTestSuite) TestCpcArrayUpdate2Jud() {
	r, e := suite.tairClient.CpcUpdate(ctx, key, item).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.CpcUpdate(ctx, key, item2).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "OK")

	r2, e2 := suite.tairClient.CpcUpdate(ctx, key, item3).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "OK")

	r3, e3 := suite.tairClient.CpcEstimate(ctx, key).Result()
	assert.NoError(suite.T(), e3)
	assert.InDelta(suite.T(), r3, 3, 0.001)

	r4, e4 := suite.tairClient.CpcUpdate2Jud(ctx, key, item).Result()
	assert.NoError(suite.T(), e4)
	assert.InDelta(suite.T(), r4.Value(), 3.00, 0.001)
	assert.InDelta(suite.T(), r4.DiffValue(), 0.00, 0.001)

	r5, e5 := suite.tairClient.CpcUpdate2Jud(ctx, key, item4).Result()
	assert.NoError(suite.T(), e5)
	assert.InDelta(suite.T(), r5.Value(), 4.00, 0.001)
	assert.InDelta(suite.T(), r5.DiffValue(), 1.00, 0.001)
}

func TestTairCpcTestSuite(t *testing.T) {
	suite.Run(t, new(TairCpcTestSuite))
}
