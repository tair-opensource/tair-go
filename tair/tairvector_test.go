package tair_test

import (
	"encoding/json"
	"fmt"
	"github.com/alibaba/tair-go/tair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"
)

type TairVectorTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairVectorTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairVectorTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func createIndex(suite *TairVectorTestSuite, index string, dim int, algorithm string, distanceMethod string, args *tair.TvsCreateIndexArgs) {
	result, err := suite.tairClient.TvsCreateIndex(ctx, index, dim, algorithm, distanceMethod, args).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, "OK")
}

func delIndex(suite *TairVectorTestSuite, index string) {
	result, err := suite.tairClient.TvsDelIndex(ctx, index).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(1))
}

func hSet(suite *TairVectorTestSuite, index string, key string, dim int) int64 {
	fields := make(map[string]interface{})
	fields["field1"] = "value1"
	fields["field2"] = rand.Intn(100)
	fields["field3"] = rand.Float32()
	fields["field4"] = rand.Intn(2) == 0
	floats := make([]float32, dim)
	for i := 0; i < dim; i++ {
		floats[i] = rand.Float32()
	}
	b, _ := json.Marshal(floats)
	fields["VECTOR"] = string(b)

	result, err := suite.tairClient.TvsHSet(ctx, index, key, tair.TvsHSetArgs{}.New().Fields(fields)).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(len(fields)))
	return result
}

func (suite *TairVectorTestSuite) TestTvsCreateAndDelIndex() {
	index := "test_index1"
	createIndex(suite, index, 32, "HNSW", "L2", nil)
	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsGetIndex() {
	index := "test_index2"
	args := tair.TvsCreateIndexArgs{}.New().DataType("FLOAT16").EfConstruct(200).M(32).AutoGc(true)
	createIndex(suite, index, 32, "HNSW", "L2", args)

	result, err := suite.tairClient.TvsGetIndex(ctx, index).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), len(result), 24)

	m := make(map[string]interface{})
	for i := 0; i < len(result); i += 2 {
		key, ok1 := result[i].(string)
		value := result[i+1]
		if ok1 {
			m[key] = value
		}
	}
	assert.Equal(suite.T(), m["algorithm"], "HNSW")
	assert.Equal(suite.T(), m["distance_method"], "L2")
	assert.Equal(suite.T(), m["data_type"], "FLOAT16")
	assert.Equal(suite.T(), m["dimension"], "32")
	assert.Equal(suite.T(), m["ef_construct"], "200")
	assert.Equal(suite.T(), m["auto_gc"], "1")
	assert.Equal(suite.T(), m["current_record_count"], "0")

	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsScanIndex() {
	indexs := []string{"index1", "index2", "index3"}
	for _, index := range indexs {
		createIndex(suite, index, 32, "HNSW", "L2", nil)
	}

	args := tair.TvsScanIndexArgs{}.New().Count(10).Pattern("index*")
	result, err := suite.tairClient.TvsScanIndex(ctx, "0", args).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 2, len(result), 2)
	assert.Equal(suite.T(), "0", result[0])

	newValue := result[1].([]interface{})
	sort.Slice(newValue, func(i, j int) bool {
		return newValue[i].(string) < newValue[j].(string)
	})
	assert.Equal(suite.T(), append(make([]interface{}, 0), "index1", "index2", "index3"), newValue)

	for _, index := range indexs {
		delIndex(suite, index)
	}
}

func (suite *TairVectorTestSuite) TestTvsHSet() {
	index := "test_index3"
	createIndex(suite, index, 4, "HNSW", "L2", nil)

	key := "test_key"
	hSet(suite, index, key, 4)
	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsHGetAllHMGet() {
	index := "test_index4"
	createIndex(suite, index, 4, "HNSW", "L2", nil)

	key := "test_key"
	fieldsLen := hSet(suite, index, key, 4)

	result, err := suite.tairClient.TvsHGetAll(ctx, index, key).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), fieldsLen*2, int64(len(result)))

	result, err = suite.tairClient.TvsHMGet(ctx, index, key, []string{"VECTOR", "field1"}).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), len(result), 2)
	assert.Equal(suite.T(), result[1], "value1")

	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsDelHDel() {
	index := "test_index5"
	createIndex(suite, index, 4, "HNSW", "L2", nil)

	key := "test_key"
	fieldsLen := hSet(suite, index, key, 4)

	result, err := suite.tairClient.TvsHDel(ctx, index, key, []string{"field1", "field2"}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(2))

	result2, err2 := suite.tairClient.TvsHGetAll(ctx, index, key).Result()
	assert.NoError(suite.T(), err2)
	assert.NotNil(suite.T(), result2)
	assert.Equal(suite.T(), (fieldsLen-2)*2, int64(len(result2)))

	result, err = suite.tairClient.TvsDel(ctx, index, []string{key}).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(1))

	result2, err2 = suite.tairClient.TvsHGetAll(ctx, index, key).Result()
	assert.NoError(suite.T(), err2)
	assert.NotNil(suite.T(), result2)
	assert.Equal(suite.T(), int64(0), int64(len(result2)))

	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsScan() {
	index := "test_index6"
	createIndex(suite, index, 4, "HNSW", "L2", nil)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		count := hSet(suite, index, key, 4)
		result, err := suite.tairClient.TvsHGetAll(ctx, index, key).Result()
		assert.NoError(suite.T(), err)
		assert.NotNil(suite.T(), result)
		assert.Equal(suite.T(), (count)*2, int64(len(result)))
	}

	// 扫描
	cursor := "0"
	keys := make([]string, 0)
	for {
		result, err := suite.tairClient.TvsScan(ctx, index, cursor,
			tair.TvsScanArgs{}.New().Count(10).Pattern("test_key_*").Filter("field2 > 50")).Result()
		assert.NoError(suite.T(), err)
		assert.NotNil(suite.T(), result)

		cursor = result[0].(string)
		for _, key := range result[1].([]interface{}) {
			keys = append(keys, key.(string))
		}
		if cursor == "0" {
			break
		}
	}

	// 验证 field2 > 50
	for _, key := range keys {
		result, err := suite.tairClient.TvsHMGet(ctx, index, key, []string{"field2"}).Result()
		assert.NoError(suite.T(), err)
		assert.Equal(suite.T(), 1, len(result))
		intValue, err := strconv.Atoi(result[0].(string))
		assert.Greater(suite.T(), intValue, 50)
	}

	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsHIncr() {
	index := "test_index7"
	createIndex(suite, index, 4, "HNSW", "L2", nil)
	key := "test_key"
	fields := make(map[string]interface{})
	fields["field1"] = "hello"
	fields["field2"] = 10
	fields["field3"] = 3.14
	fields["field4"] = true
	fields["VECTOR"] = "[1.1, 2.2, 3.3, 4.4]"
	result, e := suite.tairClient.TvsHSet(ctx, index, key, tair.TvsHSetArgs{}.New().Fields(fields)).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), result, int64(len(fields)))

	result, err := suite.tairClient.TvsHIncrBy(ctx, index, key, "field2", 1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(10+1))

	result2, err2 := suite.tairClient.TvsHIncrByFloat(ctx, index, key, "field3", 1.0).Result()
	assert.NoError(suite.T(), err2)
	assert.True(suite.T(), math.Abs(result2-3.14-1.0) < 0.0001)

	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsExpire() {
	index := "test_index8"
	createIndex(suite, index, 4, "HNSW", "L2", nil)

	keys := []string{"key1", "key2", "key3", "key4"}
	for _, key := range keys {
		_ = hSet(suite, index, key, 4)
	}

	result, err := suite.tairClient.TvsHPExpire(ctx, index, "key1", 1000).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(1))

	result, err = suite.tairClient.TvsHPTTL(ctx, index, "key1").Result()
	assert.NoError(suite.T(), err)
	assert.LessOrEqual(suite.T(), result, int64(1000))

	millTimeStamp := time.Now().UnixNano()/int64(time.Millisecond) + 1000
	result, err = suite.tairClient.TvsHPExpireAt(ctx, index, "key2", int(millTimeStamp)).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(1))

	result, err = suite.tairClient.TvsHPExpireTime(ctx, index, "key2").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, millTimeStamp)

	result, err = suite.tairClient.TvsHExpire(ctx, index, "key3", 1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(1))

	result, err = suite.tairClient.TvsHTTL(ctx, index, "key3").Result()
	assert.NoError(suite.T(), err)
	assert.LessOrEqual(suite.T(), result, int64(1))

	timestamp := time.Now().Unix() + 1
	result, err = suite.tairClient.TvsHExpireAt(ctx, index, "key4", int(timestamp)).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(1))

	result, err = suite.tairClient.TvsHExpireTime(ctx, index, "key4").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, timestamp)

	time.Sleep(time.Duration(2000) * time.Millisecond)

	for _, key := range keys {
		result, err = suite.tairClient.TvsHPTTL(ctx, index, key).Result()
		assert.NoError(suite.T(), err)
		assert.Equal(suite.T(), result, int64(-2))
	}

	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsKnnSearch() {
	index := "test_index9"
	createIndex(suite, index, 4, "HNSW", "L2", nil)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		count := hSet(suite, index, key, 4)
		result, err := suite.tairClient.TvsHGetAll(ctx, index, key).Result()
		assert.NoError(suite.T(), err)
		assert.NotNil(suite.T(), result)
		assert.Equal(suite.T(), (count)*2, int64(len(result)))
	}

	// 10 knnSearch result
	result, err := suite.tairClient.TvsKnnSearch(ctx, index, 10, "[0, 0, 0, 0]", nil).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 10*2, len(result))

	// empty result
	result, err = suite.tairClient.TvsKnnSearch(ctx, index, 10, "[0, 0, 0, 0]",
		tair.TvsKnnSearchArgs{}.New().Filter("field2 > 100")).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 0, len(result))

	// max_dist
	result, err = suite.tairClient.TvsKnnSearch(ctx, index, 10, "[0, 0, 0, 0]",
		tair.TvsKnnSearchArgs{}.New().Filter("field2 > 50").MaxDist(0.2)).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	for i := 0; i < len(result); i += 2 {
		_ = result[i].(string)
		dist, _ := strconv.ParseFloat(result[i+1].(string), 32)
		assert.Less(suite.T(), dist, 0.2)
	}

	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsGetDistance() {
	index := "test_index10"
	createIndex(suite, index, 4, "HNSW", "L2", nil)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		count := hSet(suite, index, key, 4)
		result, err := suite.tairClient.TvsHGetAll(ctx, index, key).Result()
		assert.NoError(suite.T(), err)
		assert.NotNil(suite.T(), result)
		assert.Equal(suite.T(), (count)*2, int64(len(result)))
	}

	result, err := suite.tairClient.TvsGetDistance(ctx, index, "[0,0,0,0]",
		[]string{"test_key_1", "test_key_2", "no_exist_key"}, tair.TvsGetDistanceArgs{}.New().MaxDist(100.0)).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 2*2, len(result))

	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsMKnnSearch() {
	index := "test_index11"
	createIndex(suite, index, 4, "HNSW", "L2", nil)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		count := hSet(suite, index, key, 4)
		result, err := suite.tairClient.TvsHGetAll(ctx, index, key).Result()
		assert.NoError(suite.T(), err)
		assert.NotNil(suite.T(), result)
		assert.Equal(suite.T(), (count)*2, int64(len(result)))
	}

	// 10 knnSearch result
	result, err := suite.tairClient.TvsMKnnSearch(ctx, index, 10,
		[]string{"[0, 0, 0, 0]", "[1,1,1,1]"}, nil).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 2, len(result))
	assert.Equal(suite.T(), 10*2, len(result[0].([]interface{})))
	assert.Equal(suite.T(), 10*2, len(result[1].([]interface{})))

	// empty result
	result, err = suite.tairClient.TvsMKnnSearch(ctx, index, 10, []string{"[0, 0, 0, 0]", "[1,1,1,1]"},
		tair.TvsKnnSearchArgs{}.New().Filter("field2 > 100")).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 2, len(result))
	assert.Equal(suite.T(), 0, len(result[0].([]interface{})))
	assert.Equal(suite.T(), 0, len(result[1].([]interface{})))

	// max_dist
	result, err = suite.tairClient.TvsMKnnSearch(ctx, index, 10, []string{"[0, 0, 0, 0]", "[1,1,1,1]"},
		tair.TvsKnnSearchArgs{}.New().Filter("field2 > 50").MaxDist(0.2)).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	for i := 0; i < 2; i++ {
		tmpResult := result[i].([]interface{})
		for j := 0; j < len(tmpResult); j += 2 {
			_ = tmpResult[j].(string)
			dist, _ := strconv.ParseFloat(tmpResult[j+1].(string), 32)
			assert.Less(suite.T(), dist, 0.2)
		}
	}

	delIndex(suite, index)
}

func (suite *TairVectorTestSuite) TestTvsMIndexKnnSearch() {
	indexes := []string{"test_index12", "test_index13"}
	for _, index := range indexes {
		createIndex(suite, index, 4, "HNSW", "L2", nil)
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("test_key_%d", i)
			count := hSet(suite, index, key, 4)
			result, err := suite.tairClient.TvsHGetAll(ctx, index, key).Result()
			assert.NoError(suite.T(), err)
			assert.NotNil(suite.T(), result)
			assert.Equal(suite.T(), (count)*2, int64(len(result)))
		}
	}

	// 10 knnSearch result
	result, err := suite.tairClient.TvsMIndexKnnSearch(ctx, indexes, 10, "[0, 0, 0, 0]", nil).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 10*2, len(result))

	// empty result
	result, err = suite.tairClient.TvsMIndexKnnSearch(ctx, indexes, 10, "[0, 0, 0, 0]",
		tair.TvsKnnSearchArgs{}.New().Filter("field2 > 100")).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 0, len(result))

	// max_dist
	result, err = suite.tairClient.TvsMIndexKnnSearch(ctx, indexes, 10, "[0, 0, 0, 0]",
		tair.TvsKnnSearchArgs{}.New().Filter("field2 > 50").MaxDist(0.2)).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	for i := 0; i < len(result); i += 2 {
		_ = result[i].(string)
		dist, _ := strconv.ParseFloat(result[i+1].(string), 32)
		assert.Less(suite.T(), dist, 0.2)
	}

	for _, index := range indexes {
		delIndex(suite, index)
	}
}

func (suite *TairVectorTestSuite) TestTvsMIndexMKnnSearch() {
	indexes := []string{"test_index12", "test_index13"}
	for _, index := range indexes {
		createIndex(suite, index, 4, "HNSW", "L2", nil)
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("test_key_%d", i)
			count := hSet(suite, index, key, 4)
			result, err := suite.tairClient.TvsHGetAll(ctx, index, key).Result()
			assert.NoError(suite.T(), err)
			assert.NotNil(suite.T(), result)
			assert.Equal(suite.T(), (count)*2, int64(len(result)))
		}
	}

	// 10 knnSearch result
	result, err := suite.tairClient.TvsMIndexMKnnSearch(ctx, indexes, 10,
		[]string{"[0, 0, 0, 0]", "[1,1,1,1]"}, nil).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 2, len(result))
	assert.Equal(suite.T(), 10*2, len(result[0].([]interface{})))
	assert.Equal(suite.T(), 10*2, len(result[1].([]interface{})))

	// empty result
	result, err = suite.tairClient.TvsMIndexMKnnSearch(ctx, indexes, 10, []string{"[0, 0, 0, 0]", "[1,1,1,1]"},
		tair.TvsKnnSearchArgs{}.New().Filter("field2 > 100")).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	assert.Equal(suite.T(), 2, len(result))
	assert.Equal(suite.T(), 0, len(result[0].([]interface{})))
	assert.Equal(suite.T(), 0, len(result[1].([]interface{})))

	// max_dist
	result, err = suite.tairClient.TvsMIndexMKnnSearch(ctx, indexes, 10, []string{"[0, 0, 0, 0]", "[1,1,1,1]"},
		tair.TvsKnnSearchArgs{}.New().Filter("field2 > 50").MaxDist(0.2)).Result()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), result)
	for i := 0; i < 2; i++ {
		tmpResult := result[i].([]interface{})
		for j := 0; j < len(tmpResult); j += 2 {
			_ = tmpResult[j].(string)
			dist, _ := strconv.ParseFloat(tmpResult[j+1].(string), 32)
			assert.Less(suite.T(), dist, 0.2)
		}
	}

	for _, index := range indexes {
		delIndex(suite, index)
	}
}

func TestTairVectorTestSuite(t *testing.T) {
	suite.Run(t, new(TairVectorTestSuite))
}
