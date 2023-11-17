package tair_test

import (
	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"math/rand"
	"testing"
	"time"
)

var randomKey = "randomPkey_" + randStrDoc(20)
var jsonKey = "jsonkey_" + randStrDoc(20)
var jsonStringExample = "{\"foo\":\"bar\",\"baz\":42}"
var jsonArrayExample = "{\"id\":[1,2,3]}"

func randStrDoc(size int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := []byte(str)
	var result []byte
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100000)))
	for i := 0; i < size; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

type TairDocTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairDocTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairDocTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairDocTestSuite) TestJsonSet() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonStringExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".").Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, jsonStringExample)

	r2, e2 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".foo").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "\"bar\"")

	r3, e3 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".baz").Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), r3, "42")
}

func (suite *TairDocTestSuite) TestJsonSetWithNxXx() {
	_, e := suite.tairClient.JsonSetArgs(ctx, jsonKey, ".", jsonStringExample, tair.JsonSetArgs{}.New().Xx()).Result()
	assert.Error(suite.T(), e)
	assert.EqualError(suite.T(), e, string(redis.Nil))

	r1, e1 := suite.tairClient.JsonSetArgs(ctx, jsonKey, ".", jsonStringExample, tair.JsonSetArgs{}.New().Nx()).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "OK")

	r2, e2 := suite.tairClient.JsonSetArgs(ctx, jsonKey, ".", jsonStringExample, tair.JsonSetArgs{}.New().Xx()).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "OK")
}

func (suite *TairDocTestSuite) TestJsonWithError() {
	_, e := suite.tairClient.JsonSet(ctx, jsonKey, "/abc", jsonStringExample).Result()
	assert.Error(suite.T(), e)
	assert.EqualError(suite.T(), e, "ERR new objects must be created at the root")
}

func (suite *TairDocTestSuite) TestJsonGet() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonStringExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r2, e2 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, jsonStringExample)

	r3, e3 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".foo").Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), r3, "\"bar\"")

	r4, e4 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".baz").Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), r4, "42")

	_, e5 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".non-exist").Result()
	assert.Error(suite.T(), e5)
	assert.Contains(suite.T(), e5, "ERR pointer illegal")
}

func (suite *TairDocTestSuite) TestJsonGetGetWithXmlAndYaml() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonStringExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r2, e2 := suite.tairClient.JsonGetArgs(ctx, jsonKey, ".", tair.JsonGetArgs{}.New().Format("xml")).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><foo>bar</foo><baz>42</baz></root>")

	r3, e3 := suite.tairClient.JsonGetArgs(ctx, jsonKey, ".", tair.JsonGetArgs{}.New().Format("yaml")).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), r3, "\nfoo: bar\nbaz: 42\n")
}

func (suite *TairDocTestSuite) TestJsonDel() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonStringExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonDelPath(ctx, jsonKey, ".foo").Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(1))

	_, e2 := suite.tairClient.JsonDelPath(ctx, jsonKey, ".non-exist").Result()
	assert.Error(suite.T(), e2)
	assert.Contains(suite.T(), e2, "ERR old item is null")

	r3, e3 := suite.tairClient.JsonDel(ctx, jsonKey).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), r3, int64(1))

	_, e4 := suite.tairClient.JsonGet(ctx, jsonKey).Result()
	assert.Error(suite.T(), e4)
	assert.Contains(suite.T(), e4, string(redis.Nil))
}

func (suite *TairDocTestSuite) TestJsonType() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonStringExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonType(ctx, jsonKey).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "object")

	r2, e2 := suite.tairClient.JsonTypePath(ctx, jsonKey, ".baz").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "number")

	_, e3 := suite.tairClient.JsonTypePath(ctx, jsonKey, ".not-exists").Result()
	assert.Error(suite.T(), e3)
	assert.EqualError(suite.T(), e3, string(redis.Nil))
}

func (suite *TairDocTestSuite) TestJsonNumIncrBy() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonStringExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonNumIncrByWithPath(ctx, jsonKey, ".baz", 1.0).Result()
	assert.NoError(suite.T(), e1)
	assert.InDelta(suite.T(), r1, 43, 0.1)

	r2, e2 := suite.tairClient.JsonNumIncrByWithPath(ctx, jsonKey, ".baz", 1.5).Result()
	assert.NoError(suite.T(), e2)
	assert.InDelta(suite.T(), r2, 44.5, 0.1)

	_, e3 := suite.tairClient.JsonNumIncrByWithPath(ctx, jsonKey, ".foo", 1.5).Result()
	assert.Error(suite.T(), e3)
	assert.Contains(suite.T(), e3, "ERR node not exists")
}

func (suite *TairDocTestSuite) TestJsonStrAppend() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonStringExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonStrAppendWithPath(ctx, jsonKey, ".foo", "rrrr").Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(7))

	r2, e2 := suite.tairClient.JsonStrAppendWithPath(ctx, jsonKey, ".foo", "tttt").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, int64(11))

	r4, e4 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".foo").Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), r4, "\"barrrrrtttt\"")

	_, e3 := suite.tairClient.JsonStrAppend(ctx, jsonKey, ".not-exists").Result()
	assert.Error(suite.T(), e3)
	assert.Contains(suite.T(), e3, "ERR node not exists")
}
func (suite *TairDocTestSuite) TestJsonStrLen() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonStringExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonStrAppendWithPath(ctx, jsonKey, ".foo", "rrrrr").Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(8))

	r2, e2 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".foo").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "\"barrrrrr\"")

	r3, e3 := suite.tairClient.JsonStrLenWithPath(ctx, jsonKey, ".foo").Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), r3, int64(8))

}

func (suite *TairDocTestSuite) TestJsonArrAppend() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonArrayExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonArrAppendWithPath(ctx, jsonKey, ".id", "null", "false", "true").Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(6))

	r2, e2 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".id.2").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "3")
}

func (suite *TairDocTestSuite) TestJsonArrPop() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonArrayExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonArrPopWithPath(ctx, jsonKey, ".id", 1).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "2")

	r2, e2 := suite.tairClient.JsonArrPopWithPath(ctx, jsonKey, ".id", -1).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "3")

	_, e3 := suite.tairClient.JsonArrPopWithPath(ctx, jsonKey, ".id", 10).Result()
	assert.Error(suite.T(), e3)
	assert.Contains(suite.T(), e3, "ERR array index outflow")

	r5, e5 := suite.tairClient.JsonArrPop(ctx, jsonKey, ".id").Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), r5, "1")

	_, e4 := suite.tairClient.JsonArrPop(ctx, jsonKey, ".id").Result()
	assert.Error(suite.T(), e4)
	assert.Contains(suite.T(), e4, "ERR array index outflow")
}

func (suite *TairDocTestSuite) TestJsonArrInsert() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonArrayExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonArrInsert(ctx, jsonKey, ".id", "3", "5", "6").Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(5))

	r2, e2 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".id").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "[1,2,3,5,6]")
}

func (suite *TairDocTestSuite) TestJsonArrLen() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", jsonArrayExample).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonArrLenWithPath(ctx, jsonKey, ".id").Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(3))
}

func (suite *TairDocTestSuite) TestJsonArrLen2() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", "[1, 2, 3]").Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonArrLen(ctx, jsonKey).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(3))
}

func (suite *TairDocTestSuite) TestJsonArrTrim() {
	r, e := suite.tairClient.JsonSet(ctx, jsonKey, ".", "{\"id\":[1,2,3,4,5,6]}").Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "OK")

	r1, e1 := suite.tairClient.JsonArrTrim(ctx, jsonKey, ".id", 3, 4).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(2))

	r2, e2 := suite.tairClient.JsonGetPath(ctx, jsonKey, ".id").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "[4,5]")

	r3, e3 := suite.tairClient.JsonArrTrim(ctx, jsonKey, ".id", 0, 0).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), r3, int64(1))

	_, e4 := suite.tairClient.JsonArrTrim(ctx, jsonKey, ".id", 3, 4).Result()
	assert.Error(suite.T(), e4)
	assert.Contains(suite.T(), e4, "ERR array index outflow")
}

func (suite *TairDocTestSuite) TestJsonDelError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonDel(ctx, randomKey).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairDocTestSuite) TestJsonGetError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonGet(ctx, randomKey).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairDocTestSuite) TestJsonSetError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonSet(ctx, randomKey, ".", jsonStringExample).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairDocTestSuite) TestJsonTypeError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonType(ctx, randomKey).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairDocTestSuite) TestJsonNumIncrByError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonNumIncrBy(ctx, randomKey, 10.1).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairDocTestSuite) TestJsonStrAppendError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonStrAppend(ctx, randomKey, jsonArrayExample).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairDocTestSuite) TestJsonStrLenError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonStrLen(ctx, randomKey).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairDocTestSuite) TestJsonArrAppendError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonArrAppend(ctx, randomKey, "", "", "", "", "").Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairDocTestSuite) TestJsonArrPopError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonArrPop(ctx, randomKey, "").Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")

	_, e1 := suite.tairClient.JsonArrPop(ctx, randomKey+"no-exist", "").Result()
	assert.Error(suite.T(), e1)
	assert.Contains(suite.T(), e1, "no such key")
}

func (suite *TairDocTestSuite) TestJsonArrInsertError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonArrInsert(ctx, randomKey, ".id", "3", "5", "6").Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")

	_, e2 := suite.tairClient.JsonArrInsert(ctx, randomKey, ".id", "3", "5", "6").Result()
	assert.Error(suite.T(), e2)
	assert.Contains(suite.T(), e2, "WRONGTYPE")

	_, e3 := suite.tairClient.JsonArrInsert(ctx, randomKey, "").Result()
	assert.Error(suite.T(), e3)
	assert.Contains(suite.T(), e3, "ERR wrong number of arguments")
}

func (suite *TairDocTestSuite) TestJsonArrLenError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonArrLen(ctx, randomKey).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}
func (suite *TairDocTestSuite) TestJsonArrTrimError() {
	suite.tairClient.Set(ctx, randomKey, "bar", 0)
	_, e := suite.tairClient.JsonArrTrim(ctx, randomKey, "", 0, -1).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}
func TestTairDocTestSuite(t *testing.T) {
	suite.Run(t, new(TairDocTestSuite))
}
