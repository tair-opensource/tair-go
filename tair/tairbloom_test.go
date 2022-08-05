package tair_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/alibaba/tair-go/tair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TairBloomTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

var randomkey_ string = "randomkey_" + randStr(20)

// var randomKeyBinary_ []byte

var bbf string = "bbf" + randStr(20)

// var bcf []byte

func randStr(size int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := []byte(str)
	var result []byte
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100000)))
	for i := 0; i < size; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func (suite *TairBloomTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairBloomTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairBloomTestSuite) BeforeTest(suiteName, testName string) {
	//fmt.Printf("BeforeTest: suiteName=%s,testName=%s\n", suiteName, testName)
	//fmt.Println("dddddddd")
	//randomkey_ = "randomkey_" + randStr(20)
	////randomKeyBinary_ = []byte("randomKeyBinary_" + randStr(20))
	//bbf = "bbf" + randStr(20)
	//bcf = []byte("bcf" + randStr(20))
}

func (suite *TairBloomTestSuite) TestBfAdd() {
	r1, err1 := suite.tairClient.BfReserve(ctx, bbf, 100, 0.001).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), r1, "OK")

	r2, err2 := suite.tairClient.BfAdd(ctx, bbf, "val1").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), r2, true)

	r3, err3 := suite.tairClient.BfExists(ctx, bbf, "val1").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), r3, true)

	r4, err4 := suite.tairClient.BfExists(ctx, bbf, "val2").Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), r4, false)
}

func (suite *TairBloomTestSuite) TestBfMAdd() {
	r1, err1 := suite.tairClient.BfReserve(ctx, bbf, 100, 0.001).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), r1, "OK")

	r2, err2 := suite.tairClient.BfMAdd(ctx, bbf, "val1", "val2").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), r2[0], true)
	assert.Equal(suite.T(), r2[1], true)
}

func (suite *TairBloomTestSuite) TestBfInsert() {
	r1, err1 := suite.tairClient.BfReserve(ctx, bbf, 100, 0.001).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), r1, "OK")
	a := tair.BfInsertArgs{}.New().Capacity(100).ErrorRate(0.001)
	r4, err4 := suite.tairClient.BfInsert(ctx, bbf, a, "val1", "val2").Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), r4[0], true)
	assert.Equal(suite.T(), r4[1], true)

	r2, err2 := suite.tairClient.BfMAdd(ctx, bbf, "val3", "val4").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), r2[0], true)
	assert.Equal(suite.T(), r2[1], true)

	r3, err3 := suite.tairClient.BfMExists(ctx, bbf, "val3", "val4").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), r3[0], true)
	assert.Equal(suite.T(), r3[1], true)
	a1 := tair.BfInsertArgs{}.New().Capacity(100).ErrorRate(0.001)

	b, err := suite.tairClient.BfInsert(ctx, randomkey_, a1, "item1", "item2", "item3", "item4", "item5").Result()
	assert.NoError(suite.T(), err)
	for _, r := range b {
		assert.Equal(suite.T(), r, true)
	}

	b1, err1 := suite.tairClient.BfMExists(ctx, randomkey_, "item1", "item2", "item3", "item4", "item5").Result()
	assert.NoError(suite.T(), err1)
	for _, r := range b1 {
		assert.Equal(suite.T(), r, true)
	}
}

func (suite *TairBloomTestSuite) TestBfCommand() {
	r1, err1 := suite.tairClient.BfReserve(ctx, bbf, 100, 0.001).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), r1, "OK")

	b, err := suite.tairClient.BfAdd(ctx, randomkey_, "item1").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), b, true)

	b2, err2 := suite.tairClient.BfExists(ctx, randomkey_, "item1").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), b2, true)

	b3, err3 := suite.tairClient.BfExists(ctx, randomkey_, "item2").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), b3, false)
}

func (suite *TairBloomTestSuite) TestBfAddException() {
	suite.tairClient.BfAdd(ctx, randomkey_, randomkey_)
	suite.tairClient.Set(ctx, randomkey_, "bar", 0)
	res1, err1 := suite.tairClient.BfAdd(ctx, randomkey_, randomkey_).Result()
	assert.Error(suite.T(), err1)
	assert.Contains(suite.T(), err1, "WRONGTYPE")
	assert.Equal(suite.T(), res1, false)
}

func (suite *TairBloomTestSuite) TestBfMAddException() {
	suite.tairClient.BfMAdd(ctx, randomkey_, "item")
	suite.tairClient.Set(ctx, randomkey_, "bar", 0)
	res1, err1 := suite.tairClient.BfMAdd(ctx, randomkey_, "item").Result()
	assert.Error(suite.T(), err1)
	assert.Contains(suite.T(), err1, "WRONGTYPE")
	for _, r := range res1 {
		assert.Equal(suite.T(), r, false)
	}
}

func (suite *TairBloomTestSuite) TestBfExistException() {
	suite.tairClient.BfExists(ctx, randomkey_, "item")
	suite.tairClient.Set(ctx, randomkey_, "bar", 0)
	res1, err1 := suite.tairClient.BfExists(ctx, randomkey_, "item").Result()
	assert.Error(suite.T(), err1)
	assert.Contains(suite.T(), err1, "WRONGTYPE")
	assert.Equal(suite.T(), res1, false)
}

func (suite *TairBloomTestSuite) TestBfMExistException() {
	suite.tairClient.BfMExists(ctx, randomkey_, "item")
	suite.tairClient.Set(ctx, randomkey_, "bar", 0)
	res1, err1 := suite.tairClient.BfMExists(ctx, randomkey_, "item").Result()
	assert.Error(suite.T(), err1)
	assert.Contains(suite.T(), err1, "WRONGTYPE")
	for _, r := range res1 {
		assert.Equal(suite.T(), r, false)
	}
}

func (suite *TairBloomTestSuite) TestBfInsertException() {
	suite.tairClient.BfInsert(ctx, randomkey_, tair.BfInsertArgs{}.New(), "item")
	suite.tairClient.Set(ctx, randomkey_, "bar", 0)
	_, err1 := suite.tairClient.BfInsert(ctx, randomkey_, tair.BfInsertArgs{}.New(), "item").Result()
	assert.Error(suite.T(), err1)
	assert.Contains(suite.T(), err1, "WRONGTYPE")
}

func (suite *TairBloomTestSuite) TestBfReserveException() {
	suite.tairClient.BfReserve(ctx, randomkey_, 1, 0.01)
	suite.tairClient.Set(ctx, randomkey_, "bar", 0)
	res1, err1 := suite.tairClient.BfReserve(ctx, randomkey_, 1, 0.01).Result()
	assert.Error(suite.T(), err1)
	assert.Contains(suite.T(), err1, "WRONGTYPE")
	for _, r := range res1 {
		assert.Equal(suite.T(), r, false)
	}
}

func (suite *TairBloomTestSuite) TestBfDebugException() {
	suite.tairClient.BfDebug(ctx, randomkey_)
	suite.tairClient.Set(ctx, randomkey_, "bar", 0)
	_, err1 := suite.tairClient.BfDebug(ctx, randomkey_).Result()
	assert.Error(suite.T(), err1)
	assert.Contains(suite.T(), err1, "WRONGTYPE")
}

func TestTairBloomTestSuite(t *testing.T) {
	suite.Run(t, new(TairBloomTestSuite))
}
