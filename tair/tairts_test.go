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

type TairTsTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

var randomPKey = "randomPkey_" + randStrTs(20)
var randomPKey1 = "randomPkey_" + randStrTs(20)
var randomSKey = "key" + randStrTs(20)
var randomSKey1 = "key" + randStrTs(20)
var randomSKey2 = "key2" + randStrTs(20)

var startTs = (time.Now().UnixMilli() - 1000000) / 1000 * 1000
var startTsStr = strconv.FormatInt(startTs, 10)
var startTs1 = (time.Now().UnixMilli() - 1000000) / 1000 * 1000
var endTs = (time.Now().UnixMilli()) / 1000 * 1000
var endTsStr = strconv.FormatInt(endTs, 10)
var endTs1 = (time.Now().UnixMilli()) / 1000 * 1000

func randStrTs(size int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := []byte(str)
	var result []byte
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100000)))
	for i := 0; i < size; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func (suite *TairTsTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairTsTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairTsTestSuite) TestExTsAdd() {
	{
		val := 0.0
		ts := startTs
		args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
		labels := []string{"label1", "1", "label2", "2"}
		args.Labels(labels)

		r, e := suite.tairClient.ExTsAdd(ctx, randomPKey, randomSKey, strconv.Itoa(int(ts)), val).Result()
		assert.NoError(suite.T(), e)
		assert.Equal(suite.T(), r, "OK")

		ts = ts + 1
		r1, e1 := suite.tairClient.ExTsAddArgs(ctx, randomPKey, randomSKey, strconv.Itoa(int(ts)), val, args).Result()
		assert.NoError(suite.T(), e1)
		assert.Equal(suite.T(), r1, "OK")
	}

	{
		val := 0.0
		ts := startTs
		args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
		labels := []string{"label1", "1", "label2", "2"}
		args.Labels(labels)

		r, e := suite.tairClient.ExTsAdd(ctx, randomPKey1, randomSKey1, strconv.Itoa(int(ts)), val).Result()
		assert.NoError(suite.T(), e)
		assert.Equal(suite.T(), r, "OK")

		ts = ts + 1
		r1, e1 := suite.tairClient.ExTsAddArgs(ctx, randomPKey1, randomSKey1, strconv.Itoa(int(ts)), val, args).Result()
		assert.NoError(suite.T(), e1)
		assert.Equal(suite.T(), r1, "OK")
	}
}

func (suite *TairTsTestSuite) TestExTsAlter() {

	val := 0.0
	ts := startTs
	args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
	labels := []string{"label1", "1", "label2", "2"}
	args.Labels(labels)

	r, e := suite.tairClient.ExTsAddArgs(ctx, randomPKey, randomSKey, strconv.Itoa(int(ts)), val, args).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	ts = ts + 1
	r1, e1 := suite.tairClient.ExTsAddArgs(ctx, randomPKey, randomSKey, strconv.Itoa(int(ts)), val, args).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "OK")

	filters1 := []*tair.ExTsFilter{
		(&tair.ExTsFilter{}).SetFilter("label1=1"),
		(&tair.ExTsFilter{}).SetFilter("label2=2"),
	}

	r2, e2 := suite.tairClient.ExTsMRange(ctx, randomPKey, strconv.FormatInt(startTs, 10), strconv.FormatInt(endTs, 10), filters1).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2[0].SKey(), randomSKey)
	exTsLabels := r2[0].Labels()
	assert.Equal(suite.T(), len(exTsLabels), 0)

	filters2 := []*tair.ExTsFilter{
		(&tair.ExTsFilter{}).SetFilter("label3=3"),
		(&tair.ExTsFilter{}).SetFilter("label4=4"),
	}

	args1 := tair.ExTsAttributeArgs{}.New().Labels([]string{"label3", "3", "label4", "4"})
	r5, e5 := suite.tairClient.ExTsAlter(ctx, randomPKey, randomSKey, args1).Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), r5, "OK")

	r3, e3 := suite.tairClient.ExTsMRange(ctx, randomPKey, strconv.FormatInt(startTs, 10), strconv.FormatInt(endTs, 10), filters1).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), len(r3), 0)
	r4, e4 := suite.tairClient.ExTsMRange(ctx, randomPKey, strconv.FormatInt(startTs, 10), strconv.FormatInt(endTs, 10), filters2).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), len(r4), 1)
}

func (suite *TairTsTestSuite) TestExTsRawModify() {
	val := 0.0
	ts := startTs
	tsStr := strconv.FormatInt(startTs, 10)
	args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
	labels := []string{"label1", "1", "label2", "2"}
	args.Labels(labels)

	r, e := suite.tairClient.ExTsRawModify(ctx, randomPKey, randomSKey, tsStr, val).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.ExTsRawModify(ctx, randomPKey, randomSKey, tsStr, val).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "OK")

	r2 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey)
	assert.NoError(suite.T(), r2.Err())
	assert.Equal(suite.T(), r2.Ts(), ts)
	assert.InDelta(suite.T(), r2.Value(), val, 0.0)

	ts = ts + 1
	tsStr = strconv.FormatInt(ts, 10)
	r3, e3 := suite.tairClient.ExTsRawModifyArgs(ctx, randomPKey, randomSKey, tsStr, val, args).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), r3, "OK")

	r4, e4 := suite.tairClient.ExTsRawModifyArgs(ctx, randomPKey, randomSKey, tsStr, val, args).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), r4, "OK")

	r5 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey)
	assert.NoError(suite.T(), r5.Err())
	assert.Equal(suite.T(), r5.Ts(), ts)
	assert.InDelta(suite.T(), r5.Value(), val, 0.0)
}

func (suite *TairTsTestSuite) TestExTsMAddTest() {
	val := 0.0
	ts := startTs
	args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
	labels := []string{"label1", "1", "label2", "2"}
	args.Labels(labels)

	points := []*tair.ExTsDataPoint{
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey).SetTs(strconv.FormatInt(ts, 10)).SetValue(val),
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey2).SetTs(strconv.FormatInt(ts, 10)).SetValue(val),
	}
	r, e := suite.tairClient.ExTsMAdd(ctx, randomPKey, points).Result()
	assert.NoError(suite.T(), e)
	for _, res := range r {
		assert.Equal(suite.T(), res, "OK")
	}

	r1, e1 := suite.tairClient.ExTsDel(ctx, randomPKey, randomSKey).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "OK")

	r2, e2 := suite.tairClient.ExTsDel(ctx, randomPKey, randomSKey2).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "OK")

	r3, e3 := suite.tairClient.ExTsMAddArgs(ctx, randomPKey, points, args).Result()
	assert.NoError(suite.T(), e3)
	for _, res := range r3 {
		assert.Equal(suite.T(), res, "OK")
	}

	r4, e4 := suite.tairClient.ExTsDel(ctx, randomPKey, randomSKey).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), r4, "OK")

	r5, e5 := suite.tairClient.ExTsDel(ctx, randomPKey, randomSKey2).Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), r5, "OK")
}

func (suite *TairTsTestSuite) TestExTsRawMModify() {
	val := 0.0
	ts := startTs
	tsStr := strconv.FormatInt(startTs, 10)
	args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
	labels := []string{"label1", "1"}
	args.Labels(labels)

	points := []*tair.ExTsDataPoint{
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey).SetTs(tsStr).SetValue(val),
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey2).SetTs(tsStr).SetValue(val),
	}

	r, e := suite.tairClient.ExTsMRawModify(ctx, randomPKey, points).Result()
	assert.NoError(suite.T(), e)
	for _, res := range r {
		assert.Equal(suite.T(), res, "OK")

	}
	r2, e2 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2.Ts(), ts)
	assert.InDelta(suite.T(), r2.Value(), 0, 0.0)

	r3 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey2)
	assert.NoError(suite.T(), r3.Err())
	assert.Equal(suite.T(), r3.Ts(), ts)
	assert.InDelta(suite.T(), r3.Value(), 0, 0.0)

	ts = ts + 1
	tsStr1 := strconv.FormatInt(ts, 10)
	points1 := []*tair.ExTsDataPoint{
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey).SetTs(tsStr1).SetValue(val),
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey2).SetTs(tsStr1).SetValue(val),
	}

	r4, e4 := suite.tairClient.ExTsMRawModifyArgs(ctx, randomPKey, points1, args).Result()
	assert.NoError(suite.T(), e4)
	for _, res := range r4 {
		assert.Equal(suite.T(), res, "OK")
	}

	r5, e5 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey).Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), r5.Ts(), ts)
	assert.InDelta(suite.T(), r5.Value(), 0, 0.0)

	r6, e6 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey2).Result()
	assert.NoError(suite.T(), e6)
	assert.Equal(suite.T(), r6.Ts(), ts)
	assert.InDelta(suite.T(), r6.Value(), 0, 0.0)

	r7, e7 := suite.tairClient.ExTsDel(ctx, randomPKey, randomSKey).Result()
	assert.NoError(suite.T(), e7)
	assert.Equal(suite.T(), r7, "OK")

	r8, e8 := suite.tairClient.ExTsDel(ctx, randomPKey, randomSKey2).Result()
	assert.NoError(suite.T(), e8)
	assert.Equal(suite.T(), r8, "OK")
}

func (suite *TairTsTestSuite) TestExTsRawMIncr() {
	val := 0.0
	ts := startTs
	tsStr := strconv.FormatInt(startTs, 10)
	args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
	labels := []string{"label1", "1"}
	args.Labels(labels)

	points := []*tair.ExTsDataPoint{
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey).SetTs(tsStr).SetValue(val),
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey2).SetTs(tsStr).SetValue(val),
	}

	r, e := suite.tairClient.ExTsMRawIncr(ctx, randomPKey, points).Result()
	assert.NoError(suite.T(), e)
	for _, res := range r {
		assert.Equal(suite.T(), res, "OK")

	}
	r2, e2 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2.Ts(), ts)
	assert.InDelta(suite.T(), r2.Value(), val, 0.0)

	r3 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey2)
	assert.NoError(suite.T(), r3.Err())
	assert.Equal(suite.T(), r3.Ts(), ts)
	assert.InDelta(suite.T(), r3.Value(), val, 0.0)

	ts = ts + 1
	val = val + 1
	tsStr1 := strconv.FormatInt(ts, 10)
	points1 := []*tair.ExTsDataPoint{
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey).SetTs(tsStr1).SetValue(val),
		(&tair.ExTsDataPoint{}).SetSKey(randomSKey2).SetTs(tsStr1).SetValue(val),
	}

	r4, e4 := suite.tairClient.ExTsMRawIncrArgs(ctx, randomPKey, points1, args).Result()
	assert.NoError(suite.T(), e4)
	for _, res := range r4 {
		assert.Equal(suite.T(), res, "OK")
	}

	r5, e5 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey).Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), r5.Ts(), ts)
	assert.InDelta(suite.T(), r5.Value(), val, 0.0)

	r6, e6 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey2).Result()
	assert.NoError(suite.T(), e6)
	assert.Equal(suite.T(), r6.Ts(), ts)
	assert.InDelta(suite.T(), r6.Value(), val, 0.0)

	r7, e7 := suite.tairClient.ExTsDel(ctx, randomPKey, randomSKey).Result()
	assert.NoError(suite.T(), e7)
	assert.Equal(suite.T(), r7, "OK")

	r8, e8 := suite.tairClient.ExTsDel(ctx, randomPKey, randomSKey2).Result()
	assert.NoError(suite.T(), e8)
	assert.Equal(suite.T(), r8, "OK")
}

func (suite *TairTsTestSuite) TestExTsGet() {
	val := 0.0
	tsStr := strconv.FormatInt(startTs, 10)
	args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
	labels := []string{"label1", "1", "label2", "2"}
	args.Labels(labels)

	r, e := suite.tairClient.ExTsAddArgs(ctx, randomPKey, randomSKey, tsStr, val, args).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.ExTsGet(ctx, randomPKey, randomSKey).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1.Value(), 0.0)
}
func (suite *TairTsTestSuite) TestExTsQuery() {
	val := 0.0
	tsStr := strconv.FormatInt(startTs, 10)
	args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
	labels := []string{"label1", "1", "label2", "2"}
	args.Labels(labels)

	args2 := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
	labels2 := []string{"label1", "1", "label3", "3"}
	args2.Labels(labels2)

	r, e := suite.tairClient.ExTsAddArgs(ctx, randomPKey, randomSKey, tsStr, val, args).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r2, e2 := suite.tairClient.ExTsAddArgs(ctx, randomPKey, randomSKey2, tsStr, val, args2).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "OK")

	filters := []*tair.ExTsFilter{
		(&tair.ExTsFilter{}).SetFilter("label1=1"),
		(&tair.ExTsFilter{}).SetFilter("label2=2"),
	}

	r3, e3 := suite.tairClient.ExTsQuery(ctx, randomPKey, filters).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), len(r3), 1)
	assert.Equal(suite.T(), r3[0], randomSKey)

	filters2 := []*tair.ExTsFilter{
		(&tair.ExTsFilter{}).SetFilter("label1=1"),
		(&tair.ExTsFilter{}).SetFilter("label3=3"),
	}
	r4, e4 := suite.tairClient.ExTsQuery(ctx, randomPKey, filters2).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), len(r4), 1)
	assert.Equal(suite.T(), r4[0], randomSKey2)

	filters3 := []*tair.ExTsFilter{
		(&tair.ExTsFilter{}).SetFilter("label1=1"),
	}

	r5, e5 := suite.tairClient.ExTsQuery(ctx, randomPKey, filters3).Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), len(r5), 2)

	filters4 := []*tair.ExTsFilter{
		(&tair.ExTsFilter{}).SetFilter("label2=3"),
	}
	r6, e6 := suite.tairClient.ExTsQuery(ctx, randomPKey, filters4).Result()
	assert.NoError(suite.T(), e6)
	assert.Equal(suite.T(), len(r6), 0)
}

func (suite *TairTsTestSuite) TestExTsRange() {
	num := 3
	for i := 0; i < num; i++ {
		val := float64(i)
		ts := startTs + int64(i*1000)
		tsStr := strconv.FormatInt(ts, 10)
		args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
		labels := []string{"label1", "1", "label2", "2"}
		args.Labels(labels)

		r, e := suite.tairClient.ExTsAddArgs(ctx, randomPKey, randomSKey, tsStr, val, args).Result()
		assert.NoError(suite.T(), e)
		assert.Equal(suite.T(), r, "OK")
	}

	args := tair.ExTsAggregationArgs{}.New().MaxCount(10).Avg(1000)
	r2, e2 := suite.tairClient.ExTsRangeArgs(ctx, randomPKey, randomSKey, startTsStr, endTsStr, args).Result()
	points := r2.DataPoints()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), len(points), num)

	for i := 0; i < num; i++ {
		val := float64(i)
		ts := startTs + int64(i*1000)
		assert.Equal(suite.T(), points[i].Ts(), ts)
		assert.InDelta(suite.T(), points[i].Value(), val, 0.0)
	}
}

//func (suite *TairTsTestSuite) TestExTsMRange() {
//	num := 3
//	for i := 0; i < num; i++ {
//		val := float64(i)
//		ts := startTs + int64(i*1000)
//		tsStr := strconv.FormatInt(ts, 10)
//		args := tair.ExTsAttributeArgs{}.New().DataEt(1000000000).ChunkSize(1024).UnCompressed()
//		labels := []string{"label1", "1", "label2", "2"}
//		args.Labels(labels)
//
//		r, e := suite.tairClient.ExTsAddArgs(ctx, randomPKey, randomSKey, tsStr, val, args).Result()
//		assert.NoError(suite.T(), e)
//		assert.Equal(suite.T(), r, "OK")
//
//		r1, e1 := suite.tairClient.ExTsAddArgs(ctx, randomPKey, randomSKey2, tsStr, val, args).Result()
//		assert.NoError(suite.T(), e1)
//		assert.Equal(suite.T(), r1, "OK")
//	}
//
//	args := tair.ExTsAggregationArgs{}.New().MaxCount(10).Avg(1000)
//	keys := [...]string{randomSKey, randomSKey2}
//
//	r2, e2 := suite.tairClient.ExTsMRangeArgs(ctx, randomPKey, keys, startTsStr, endTsStr, args).Result()
//	points := r2.DataPoints()
//	assert.NoError(suite.T(), e2)
//	assert.Equal(suite.T(), len(points), num)
//
//	for i := 0; i < num; i++ {
//		val := float64(i)
//		ts := startTs + int64(i*1000)
//		assert.Equal(suite.T(), points[i].Ts(), ts)
//		assert.InDelta(suite.T(), points[i].Value(), val, 0.0)
//	}
//}

func TestTairTsTestSuite(t *testing.T) {
	suite.Run(t, new(TairTsTestSuite))
}
