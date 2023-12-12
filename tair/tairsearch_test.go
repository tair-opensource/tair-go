package tair_test

import (
	"encoding/json"
	"github.com/alibaba/tair-go/tair"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"regexp"
	"testing"
)

type TairSearchTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairSearchTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairSearchTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairSearchTestSuite) TestTftCreateIndex() {
	result, err := suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\","+
		"\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, "OK")
}

func (suite *TairSearchTestSuite) TestTftUpdateIndex() {
	result, err := suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, "OK")

	result1, err1 := suite.tairClient.TftGetIndexMappings(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, "{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}")

	result2, err2 := suite.tairClient.TftUpdateIndex(ctx, "tftkey", "{\"mappings\":{\"properties\":{\"f1\":{\"type\":\"text\"}}}}").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, "OK")

	result3, err3 := suite.tairClient.TftGetIndexMappings(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, "{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}")

	result4, err4 := suite.tairClient.TftGetIndex(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), result4, "{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}},\"settings\":{}}}")

	result5, err5 := suite.tairClient.TftGetIndexSettings(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err5)
	assert.Equal(suite.T(), result5, "{\"tftkey\":{\"settings\":{}}}")

	result6, err6 := suite.tairClient.TftGetIndexMappings(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err6)
	assert.Equal(suite.T(), result6, "{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}")

}

func (suite *TairSearchTestSuite) TestTftAddDoc() {
	suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5")

	result1, err1 := suite.tairClient.TftSearch(ctx, "tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}").Result()
	assert.NoError(suite.T(), err1)
	reg := regexp.MustCompile("((\"(max)?_?score|\\{\"value)\"):\\d+(\\.\\d+)?")
	res := reg.ReplaceAllString(result1, "$1:0")
	assert.Equal(suite.T(), "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":0,\"total\":{\"relation\":\"eq\",\"value\":3}}}", res)
	result, err := suite.tairClient.TftSearchUseCache(ctx, "tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}", true).Result()
	assert.NoError(suite.T(), err)
	res = reg.ReplaceAllString(result, "$1:0")
	assert.Equal(suite.T(), "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":0,\"total\":{\"relation\":\"eq\",\"value\":3}}}", res)

	result2, err2 := suite.tairClient.TftGetDoc(ctx, "tftkey", "3").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, "{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}")

	result3, err3 := suite.tairClient.TftDelDoc(ctx, "tftkey", "3").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, "1")

	_, err5 := suite.tairClient.TftGetDoc(ctx, "tftkey", "3").Result()
	assert.Error(suite.T(), err5)
	assert.Equal(suite.T(), err5, redis.Nil)

	result4, err4 := suite.tairClient.TftGetIndexMappings(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), result4, "{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}")

	result5, err5 := suite.tairClient.TftExplaincost(ctx, "tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}").Result()
	assert.NoError(suite.T(), err5)
	var results map[string]interface{}
	json.Unmarshal([]byte(result5), &results)
	_, ok := results["QUERY_COST"]
	assert.Equal(suite.T(), ok, true)
}

func (suite *TairSearchTestSuite) TestTftMSearch() {
	suite.tairClient.TftCreateIndex(ctx, "tftkey1", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	suite.tairClient.TftCreateIndex(ctx, "tftkey2", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	suite.tairClient.TftCreateIndex(ctx, "tftkey3", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey1", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey2", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey3", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey1", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey2", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5")

	result1, err1 := suite.tairClient.TftMSearch(ctx, 3, "{\"query\":{\"match\":{\"f1\":\"3\"}}}", "tftkey1", "tftkey2", "tftkey3").Result()
	assert.NoError(suite.T(), err1)
	reg := regexp.MustCompile("((\"(max)?_?score|\\{\"value)\"):\\d+(\\.\\d+)?")
	res := reg.ReplaceAllString(result1, "$1:0")
	assert.Equal(suite.T(), "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey1\",\"_score\":0,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey2\",\"_score\":0,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey3\",\"_score\":0,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":0,\"total\":{\"relation\":\"eq\",\"value\":3}},\"aux_info\":{\"index_crc64\":52600736426816810}}", res)
}

func (suite *TairSearchTestSuite) TestTftUpdateDocField() {
	result1, err := suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result1, "OK")

	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"redis is a nosql database\"}", "1")

	result2, err2 := suite.tairClient.TftSearch(ctx, "tftkey", "{\"query\":{\"term\":{\"f0\":\"redis\"}}}").Result()
	assert.NoError(suite.T(), err2)

	reg := regexp.MustCompile("((\"(max)?_?score|\\{\"value)\"):\\d+(\\.\\d+)?")
	res := reg.ReplaceAllString(result2, "$1:0")
	assert.Equal(suite.T(), "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"redis is a nosql database\"}}],\"max_score\":0,\"total\":{\"relation\":\"eq\",\"value\":1}}}", res)

	result3, err3 := suite.tairClient.TftUpdateIndex(ctx, "tftkey", "{\"mappings\":{\"properties\":{\"f1\":{\"type\":\"text\"}}}}").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, "OK")

	suite.tairClient.TftUpdateDocField(ctx, "tftkey", "1", "{\"f1\":\"mysql is a dbms\"}")
	result4, err4 := suite.tairClient.TftSearch(ctx, "tftkey", "{\"query\":{\"term\":{\"f1\":\"mysql\"}}}").Result()
	assert.NoError(suite.T(), err4)

	res = reg.ReplaceAllString(result4, "$1:0")
	assert.Equal(suite.T(), "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f1\":\"mysql is a dbms\",\"f0\":\"redis is a nosql database\"}}],\"max_score\":0,\"total\":{\"relation\":\"eq\",\"value\":1}}}", res)

	result5, err5 := suite.tairClient.TftExplainscore(ctx, "tftkey", "{\"query\":{\"term\":{\"f1\":\"mysql\"}}}").Result()
	assert.NoError(suite.T(), err5)
	reg = regexp.MustCompile("((\"(max)?_?score|\\{\"value)\"):\\d+(\\.\\d+)?")
	res = reg.ReplaceAllString(result5, "$1:0")
	assert.Equal(suite.T(), "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f1\":\"mysql is a dbms\",\"f0\":\"redis is a nosql database\"},\"_explanation\":{\"score\":0,\"description\":\"score, computed as query_boost * idf * idf * tf\",\"field\":\"f1\",\"term\":\"mysql\",\"query_boost\":1.0,\"details\":[{\"value\":0,\"description\":\"idf, computed as 1 + log(N / (n + 1))\",\"details\":[{\"value\":0,\"description\":\"n, number of documents containing term\"},{\"value\":0,\"description\":\"N, total number of documents\"}]},{\"value\":0,\"description\":\"tf, computed as sqrt(freq) / sqrt(dl)\",\"details\":[{\"value\":0,\"description\":\"freq, occurrences of term within document\"},{\"value\":0,\"description\":\"dl, length of field\"}]}]}}],\"max_score\":0,\"total\":{\"relation\":\"eq\",\"value\":1}}}", res)

	result6, err6 := suite.tairClient.TftExplainscore(ctx, "tftkey", "{\"query\":{\"term\":{\"f1\":\"mysql\"}}}", "0", "1", "2").Result()
	assert.NoError(suite.T(), err6)
	res = reg.ReplaceAllString(result6, "$1:0")
	assert.Equal(suite.T(), "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f1\":\"mysql is a dbms\",\"f0\":\"redis is a nosql database\"},\"_explanation\":{\"score\":0,\"description\":\"score, computed as query_boost * idf * idf * tf\",\"field\":\"f1\",\"term\":\"mysql\",\"query_boost\":1.0,\"details\":[{\"value\":0,\"description\":\"idf, computed as 1 + log(N / (n + 1))\",\"details\":[{\"value\":0,\"description\":\"n, number of documents containing term\"},{\"value\":0,\"description\":\"N, total number of documents\"}]},{\"value\":0,\"description\":\"tf, computed as sqrt(freq) / sqrt(dl)\",\"details\":[{\"value\":0,\"description\":\"freq, occurrences of term within document\"},{\"value\":0,\"description\":\"dl, length of field\"}]}]}}],\"max_score\":0,\"total\":{\"relation\":\"eq\",\"value\":1}}}", res)
}

func (suite *TairSearchTestSuite) TestTftIncrLongDocField() {
	_, err := suite.tairClient.TftIncrLongDocField(ctx, "tftkey", "1", "f0", 1).Result()
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err, "not exists")

	result1, err1 := suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}").Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, "OK")

	_, err2 := suite.tairClient.TftIncrLongDocField(ctx, "tftkey", "1", "f0", 1).Result()
	assert.Error(suite.T(), err2)
	assert.Contains(suite.T(), err2, "ERR incrlongdocfield only supports field of int or long type")

	suite.tairClient.Del(ctx, "tftkey")

	result3, err3 := suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, "OK")

	result4, err4 := suite.tairClient.TftIncrLongDocField(ctx, "tftkey", "1", "f0", 1).Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), result4, int64(1))

	result5, err5 := suite.tairClient.TftIncrLongDocField(ctx, "tftkey", "1", "f0", -1).Result()
	assert.NoError(suite.T(), err5)
	assert.Equal(suite.T(), result5, int64(0))

	result6, err6 := suite.tairClient.TftExists(ctx, "tftkey", "1").Result()
	assert.NoError(suite.T(), err6)
	assert.Equal(suite.T(), result6, int64(1))

}

func (suite *TairSearchTestSuite) TestTftIncrFloatDocField() {
	_, err1 := suite.tairClient.TftIncrFloatDocField(ctx, "tftkey", "1", "f0", 1.1).Result()
	assert.Error(suite.T(), err1)
	assert.Contains(suite.T(), err1, "not exists")

	result2, err2 := suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"}}}}").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, "OK")

	_, err3 := suite.tairClient.TftIncrFloatDocField(ctx, "tftkey", "1", "f0", 1.1).Result()
	assert.Error(suite.T(), err3)
	assert.Contains(suite.T(), err3, "ERR incrfloatdocfield only supports field of double type")

	suite.tairClient.Del(ctx, "tftkey")

	result4, err4 := suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"double\"}}}}").Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), result4, "OK")

	result5, err5 := suite.tairClient.TftIncrFloatDocField(ctx, "tftkey", "1", "f0", 1.1).Result()
	assert.NoError(suite.T(), err5)
	assert.InDelta(suite.T(), result5, 1.1, 0.01)

	result6, err6 := suite.tairClient.TftIncrFloatDocField(ctx, "tftkey", "1", "f0", -1.1).Result()
	assert.NoError(suite.T(), err6)
	assert.Equal(suite.T(), result6, float64(0))

	result7, err7 := suite.tairClient.TftExists(ctx, "tftkey", "1").Result()
	assert.NoError(suite.T(), err7)
	assert.Equal(suite.T(), result7, int64(1))
}

func (suite *TairSearchTestSuite) TestTftDelDocField() {
	r2, err2 := suite.tairClient.TftDelDocField(ctx, "tftkey", "1", "f0").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), r2, int64(0))

	r3, err3 := suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"long\"}}}}").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), r3, "OK")

	suite.tairClient.TftIncrLongDocField(ctx, "tftkey", "1", "f0", 1)
	suite.tairClient.TftIncrFloatDocField(ctx, "tftkey", "1", "f1", 1.1)
	r4, err4 := suite.tairClient.TftDelDocField(ctx, "tftkey", "1", "f0", "f1", "f2").Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), r4, int64(2))

}

func (suite *TairSearchTestSuite) TestTftDelDoc() {
	suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5")

	r1, err1 := suite.tairClient.TftExists(ctx, "tftkey", "3").Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), r1, int64(1))

	r2, err2 := suite.tairClient.TftDocNum(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), r2, int64(5))

	r3, err3 := suite.tairClient.TftDelDoc(ctx, "tftkey", "3", "4", "5").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), r3, "3")

	r4, err4 := suite.tairClient.TftExists(ctx, "tftkey", "3").Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), r4, int64(0))

	r5, err5 := suite.tairClient.TftDocNum(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err5)
	assert.Equal(suite.T(), r5, int64(2))

}

func (suite *TairSearchTestSuite) TestTftDelAll() {
	suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5")

	r, err := suite.tairClient.TftDelAll(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), r, "OK")

	r1, err1 := suite.tairClient.TftDocNum(ctx, "tftkey").Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), r1, int64(0))
}

func (suite *TairSearchTestSuite) TestTftScanDocId() {
	suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5")
	// todo 为 SliceCmd 增加方法。
	r1, err1 := suite.tairClient.TftScanDocId(ctx, "tftkey", "0").Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), r1[0], "0")

	res := r1[1].([]interface{})
	assert.Equal(suite.T(), len(res), 5)

	assert.Equal(suite.T(), res[0], "1")
	assert.Equal(suite.T(), res[1], "2")
	assert.Equal(suite.T(), res[2], "3")
	assert.Equal(suite.T(), res[3], "4")
	assert.Equal(suite.T(), res[4], "5")

}

func (suite *TairSearchTestSuite) TestTftScanDocIdWithCount() {
	suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5")
	// todo 修改SliceCmd 方法。
	a := tair.TftScanArgs{}.New().Count(3)
	r, e := suite.tairClient.TftScanDocIdArgs(ctx, "tftkey", "0", a).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r[0], "3")
	res := r[1].([]interface{})
	assert.Equal(suite.T(), len(res), 3)

	assert.Equal(suite.T(), res[0], "1")
	assert.Equal(suite.T(), res[1], "2")

	assert.Equal(suite.T(), res[2], "3")

	r1, e1 := suite.tairClient.TftScanDocIdArgs(ctx, "tftkey", "3", a).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1[0], "0")

	res1 := r1[1].([]interface{})
	assert.Equal(suite.T(), len(res1), 2)

	assert.Equal(suite.T(), res1[0], "4")

	assert.Equal(suite.T(), res1[1], "5")

}

func (suite *TairSearchTestSuite) TestTftScanDocIdWithMatch() {
	suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v0\",\"f1\":\"3\"}", "1_redis_doc")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v1\",\"f1\":\"3\"}", "2_redis_doc")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"3\"}", "3_mysql_doc")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"4\"}", "4_mysql_doc")
	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"v3\",\"f1\":\"5\"}", "5_tidb_doc")
	// todo
	a := tair.TftScanArgs{}.New().Match("*redis*")
	r, e := suite.tairClient.TftScanDocIdArgs(ctx, "tftkey", "0", a).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r[0], "0")
	res := r[1].([]interface{})
	assert.Equal(suite.T(), len(res), 2)

	assert.Equal(suite.T(), res[0], "1_redis_doc")
	assert.Equal(suite.T(), res[1], "2_redis_doc")

	a1 := tair.TftScanArgs{}.New().Match("*tidb*")
	r1, e1 := suite.tairClient.TftScanDocIdArgs(ctx, "tftkey", "0", a1).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1[0], "0")
	res1 := r1[1].([]interface{})
	assert.Equal(suite.T(), len(res1), 1)
	assert.Equal(suite.T(), res1[0], "5_tidb_doc")

}

func (suite *TairSearchTestSuite) TestTftAnalyzer() {
	suite.tairClient.TftCreateIndex(ctx, "tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"my_analyzer\"}}},\"settings\":{\"analysis\":{\"analyzer\":{\"my_analyzer\":{\"type\":\"standard\"}}}}}")

	text := "This is tair-go."
	r1, err1 := suite.tairClient.TftAnalyzer(ctx, "standard", text).Result()
	assert.NoError(suite.T(), err1)
	a1 := tair.TftAnalyzerArgs{}.New().Index("tftkey")
	r2, err2 := suite.tairClient.TftAnalyzerWithArgs(ctx, "my_analyzer", text, a1).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), r1, r2)

	a2 := tair.TftAnalyzerArgs{}.New().ShowTime()
	r3, err3 := suite.tairClient.TftAnalyzerWithArgs(ctx, "standard", text, a2).Result()
	assert.NoError(suite.T(), err3)
	assert.Contains(suite.T(), r3, "consuming time")
}

func (suite *TairSearchTestSuite) TestTftUnicode() {
	suite.tairClient.TftMappingIndex(ctx, "tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"chinese\"}}}}")
	r, e := suite.tairClient.TftGetIndexMappings(ctx, "tftkey").Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, "{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"analyzer\":\"chinese\",\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}")

	suite.tairClient.Del(ctx, "tftkey")

	suite.tairClient.TftMappingIndex(ctx, "tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}")
	r1, e1 := suite.tairClient.TftGetIndexMappings(ctx, "tftkey").Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}}")

	suite.tairClient.Del(ctx, "tftkey")

	suite.tairClient.TftMappingIndex(ctx, "tftkey", "{\"mappings\":{\"properties\":{\"f0\":{\"type\":\"text\",\"analyzer\":\"chinese\", \"search_analyzer\":\"chinese\"}}}}")
	r2, e2 := suite.tairClient.TftGetIndexMappings(ctx, "tftkey").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"analyzer\":\"chinese\",\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\",\"search_analyzer\":\"chinese\"}}}}}")

	suite.tairClient.TftAddDocWithId(ctx, "tftkey", "{\"f0\":\"夏天是一个很热的季节\"}", "1")
	r3, e3 := suite.tairClient.TftSearch(ctx, "tftkey", "{\"query\":{\"match\":{\"f0\":\"夏天冬天\"}}}").Result()
	assert.NoError(suite.T(), e3)

	reg := regexp.MustCompile("((\"(max)?_?score|\\{\"value)\"):\\d+(\\.\\d+)?")
	res := reg.ReplaceAllString(r3, "$1:0")
	assert.Equal(suite.T(), "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"夏天是一个很热的季节\"}}],\"max_score\":0,\"total\":{\"relation\":\"eq\",\"value\":1}}}", res)
}

func (suite *TairSearchTestSuite) TestTftMAddTestString() {
	suite.tairClient.TftMappingIndex(ctx, "tftkey", "{\"mappings\":{\"dynamic\":\"false\",\"properties\":{\"f0\":{\"type\":\"text\"},\"f1\":{\"type\":\"text\"}}}}")
	docs := make(map[string]string)
	docs["{\"f0\":\"v0\",\"f1\":\"3\"}"] = "1"
	docs["{\"f0\":\"v1\",\"f1\":\"3\"}"] = "2"
	docs["{\"f0\":\"v3\",\"f1\":\"3\"}"] = "3"
	docs["{\"f0\":\"v3\",\"f1\":\"4\"}"] = "4"
	docs["{\"f0\":\"v3\",\"f1\":\"5\"}"] = "5"

	suite.tairClient.TftMAddDoc(ctx, "tftkey", docs)

	r, e := suite.tairClient.TftSearch(ctx, "tftkey", "{\"query\":{\"match\":{\"f1\":\"3\"}}}").Result()
	assert.NoError(suite.T(), e)

	reg := regexp.MustCompile("((\"(max)?_?score|\\{\"value)\"):\\d+(\\.\\d+)?")
	res := reg.ReplaceAllString(r, "$1:0")
	assert.Equal(suite.T(), "{\"hits\":{\"hits\":[{\"_id\":\"1\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"v0\",\"f1\":\"3\"}},{\"_id\":\"2\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"v1\",\"f1\":\"3\"}},{\"_id\":\"3\",\"_index\":\"tftkey\",\"_score\":0,\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}],\"max_score\":0,\"total\":{\"relation\":\"eq\",\"value\":3}}}", res)

	r1, e1 := suite.tairClient.TftGetDoc(ctx, "tftkey", "3").Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, "{\"_id\":\"3\",\"_source\":{\"f0\":\"v3\",\"f1\":\"3\"}}")

	r2, e2 := suite.tairClient.TftDelDoc(ctx, "tftkey", "3").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "1")

	_, e3 := suite.tairClient.TftGetDoc(ctx, "tftkey", "3").Result()
	assert.Error(suite.T(), e3)
	assert.Equal(suite.T(), e3, redis.Nil)

	r4, e4 := suite.tairClient.TftGetIndexMappings(ctx, "tftkey").Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), r4, "{\"tftkey\":{\"mappings\":{\"_source\":{\"enabled\":true,\"excludes\":[],\"includes\":[]},\"dynamic\":\"false\",\"properties\":{\"f0\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"},\"f1\":{\"boost\":1.0,\"enabled\":true,\"ignore_above\":-1,\"index\":true,\"similarity\":\"classic\",\"type\":\"text\"}}}}}")
}

func (suite *TairSearchTestSuite) TestTftSug() {
	a := make(map[string]int64)
	a["redis is a memory database"] = 3
	a["redis cluster"] = 10
	r1, e1 := suite.tairClient.TftAddSug(ctx, "idx:redis", a).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(2))
	r2, e2 := suite.tairClient.TftDelSug(ctx, "idx:redis", "redis is a memory database", "redis cluster").Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, int64(2))

	a1 := make(map[string]int64)
	a1["redis is a memory database"] = 3
	a1["redis cluster"] = 10
	a1["redis lock"] = 4
	suite.tairClient.TftAddSug(ctx, "idx:redis1", a1)

	r3, e3 := suite.tairClient.TftSugSum(ctx, "idx:redis1").Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), r3, int64(3))

	a2 := make(map[string]int64)
	a2["redis is a memory database"] = 3
	a2["redis cluster"] = 10
	a2["redis lock"] = 4
	suite.tairClient.TftAddSug(ctx, "idx:redis2", a2)

	r4, e4 := suite.tairClient.TftGetSug(ctx, "idx:redis2", "res", 2, true).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), r4[0], "redis cluster")
	assert.Equal(suite.T(), r4[1], "redis lock")

	r5, e5 := suite.tairClient.TftGetAllSug(ctx, "idx:redis2").Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), r5[0], "redis cluster")
	assert.Equal(suite.T(), r5[1], "redis lock")
	assert.Equal(suite.T(), r5[2], "redis is a memory database")
}

func TestTairSearchTestSuite(t *testing.T) {
	suite.Run(t, new(TairSearchTestSuite))
}
