package tair_test

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alibaba/tair-go/tair"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

var randomGisKey = "randomPkey_" + randStrGis(20)
var area = "area_" + randStrGis(20)
var randomStr = randStrGis(20)
var bigKey = "bigKey" + randomStr

func randStrGis(size int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := []byte(str)
	var result []byte
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100000)))
	for i := 0; i < size; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

type TairGisTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairGisTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairGisTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairGisTestSuite) TestGisAddSearchContains() {
	polygonName := "alibaba-xixi-campus"
	polygonWktText := "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"
	pointWktText := "POINT (30 11)"
	r, e := suite.tairClient.GisAdd(ctx, area, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, int64(1))

	r1, e1 := suite.tairClient.GisAdd(ctx, area, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(1))

	resWktText, e1 := suite.tairClient.GisGet(ctx, area, polygonName).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), resWktText, "POLYGON((30 10,40 40,20 40,10 20,30 10))")

	r2, e2 := suite.tairClient.GisSearch(ctx, area, pointWktText).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), len(r2), 1)
	assert.Contains(suite.T(), r2, polygonName)
	assert.Equal(suite.T(), resWktText, r2[polygonName])

	r3, e3 := suite.tairClient.GisContains(ctx, area, pointWktText).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), len(r3), 1)
	assert.Contains(suite.T(), r3, polygonName)
	assert.Equal(suite.T(), resWktText, r3[polygonName])

	r4, e4 := suite.tairClient.GisIntersects(ctx, area, pointWktText).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), len(r4), 1)
	assert.Contains(suite.T(), r4, polygonName)
	assert.Equal(suite.T(), resWktText, r4[polygonName])
}

func (suite *TairGisTestSuite) TestGisDel() {
	randomStr := randStrGis(20)
	area := "shenzhen" + randomStr
	polygonName := "alibaba-xixi-campus"
	polygonWktText := "POLYGON((30 10,40 40,20 40,10 20,30 10))"
	polygonName1 := "alibaba-aliyun"
	polygonWktText1 := "POLYGON((30 10,40 40))"

	r, e := suite.tairClient.GisAdd(ctx, area, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, int64(1))

	r4, e4 := suite.tairClient.GisAdd(ctx, area, polygonName1, polygonWktText1).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), r4, int64(1))

	resWktText, err := suite.tairClient.GisGet(ctx, area, polygonName).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), polygonWktText, resWktText)

	r2, e2 := suite.tairClient.GisDel(ctx, area, polygonName).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, "OK")

	resWktText1, err := suite.tairClient.GisGet(ctx, area, polygonName1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), polygonWktText1, resWktText1)

	r3, e3 := suite.tairClient.GisSearch(ctx, area, polygonWktText1).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), len(r3), 1)
	assert.Contains(suite.T(), r3, polygonName1)
	assert.Equal(suite.T(), polygonWktText1, r3[polygonName1])
}

func (suite *TairGisTestSuite) TestGisKeyNotExistTest() {
	randomStr := randStrGis(20)
	area := "shenzhen" + randomStr
	polygonName := "alibaba-xixi-campus"
	_, e := suite.tairClient.GisDel(ctx, area, polygonName).Result()
	assert.EqualError(suite.T(), e, string(redis.Nil))
}

func (suite *TairGisTestSuite) TestGisValueNotExistTest() {
	randomStr := randStrGis(20)
	area := "shenzhen" + randomStr
	polygonName := "alibaba-xixi-campus"
	polygonWktText := "POLYGON((30 10,40 40,20 40,10 20,30 10))"
	r, e := suite.tairClient.GisAdd(ctx, area, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, int64(1))
	_, e1 := suite.tairClient.GisDel(ctx, area, "not-exists-polygonName").Result()
	assert.EqualError(suite.T(), e1, string(redis.Nil))
}

func (suite *TairGisTestSuite) TestGisKeyTypeTest() {
	randomStr := randStrGis(20)
	area := "shenzhen" + randomStr
	suite.tairClient.Set(ctx, area, "value", 0)
	_, e1 := suite.tairClient.GisDel(ctx, area, "not-exists-polygonName").Result()
	assert.Contains(suite.T(), e1, "WRONGTYPE")
}

func (suite *TairGisTestSuite) TestGisBigKeyTest() {
	polygonName := randStrGis(40)
	builder := strings.Builder{}
	builder.WriteString("POLYGON ((")
	maxLen := 100000
	for i := 0; i < maxLen; i++ {
		x := rand.Intn(1024)
		y := rand.Intn(1024)
		builder.WriteString(strconv.Itoa(x))
		builder.WriteString(" ")
		builder.WriteString(strconv.Itoa(y))
		if i < maxLen-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString("))")
	polygonWktText := builder.String()
	r, e := suite.tairClient.GisAdd(ctx, bigKey, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, int64(1))
}

func (suite *TairGisTestSuite) TestGisSearchBigKeyTest() {

	polygonName := randStrGis(40)
	builder := strings.Builder{}
	builder.WriteString("POLYGON ((")
	maxLen := 100000
	for i := 0; i < maxLen; i++ {
		x := rand.Intn(1024)
		y := rand.Intn(1024)
		builder.WriteString(strconv.Itoa(x))
		builder.WriteString(" ")
		builder.WriteString(strconv.Itoa(y))
		if i < maxLen-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString("))")
	polygonWktText := builder.String()
	r, e := suite.tairClient.GisAdd(ctx, bigKey, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, int64(1))

	pointWktText := "POINT (30 11)"
	linestringWktText := "LINESTRING (10 10, 40 40)"
	polygonWktText1 := "POLYGON ((31 20, 29 20, 29 21, 31 31))"

	suite.tairClient.GisSearch(ctx, bigKey, pointWktText)
	suite.tairClient.GisContains(ctx, bigKey, pointWktText)
	suite.tairClient.GisIntersects(ctx, bigKey, pointWktText)

	suite.tairClient.GisSearch(ctx, bigKey, linestringWktText)
	suite.tairClient.GisContains(ctx, bigKey, linestringWktText)
	suite.tairClient.GisIntersects(ctx, bigKey, linestringWktText)

	suite.tairClient.GisSearch(ctx, bigKey, polygonWktText1)
	suite.tairClient.GisContains(ctx, bigKey, polygonWktText1)
	suite.tairClient.GisIntersects(ctx, bigKey, polygonWktText1)
}

func (suite *TairGisTestSuite) TestGisContains() {
	randomStr1 := randStrGis(20)
	key := "shenzhen" + randomStr1
	polygonName := "alibaba-xixi-campus"
	polygonWktText := "POLYGON((30 10,40 40,20 40,10 20,30 10))"
	pointWkt := "POINT (30 11)"

	r, e := suite.tairClient.GisAdd(ctx, key, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, int64(1))

	r1, e1 := suite.tairClient.GisContains(ctx, key, pointWkt).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), len(r1), 1)
	assert.Contains(suite.T(), r1, polygonName)
	assert.Equal(suite.T(), polygonWktText, r1[polygonName])

	r2, e2 := suite.tairClient.GisContainsArgs(ctx, key, pointWkt, tair.GisArgs{}.NewGisArgs().WithoutWkt()).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), len(r2), 1)
	assert.Contains(suite.T(), r2[0], polygonName)
}

func (suite *TairGisTestSuite) TestGisGetAll() {
	randomStr1 := randStrGis(20)
	key := "shenzhen" + randomStr1
	polygonName := "alibaba-xixi-campus"
	polygonWktText := "POLYGON((30 10,40 40,20 40,10 20,30 10))"

	r, e := suite.tairClient.GisAdd(ctx, key, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, int64(1))

	r1, e1 := suite.tairClient.GisGetAll(ctx, key).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), len(r1), 1)
	assert.Contains(suite.T(), r1, polygonName)
	assert.Equal(suite.T(), polygonWktText, r1[polygonName])

	r2, e2 := suite.tairClient.GisGetAllArgs(ctx, key, tair.GisArgs{}.NewGisArgs().WithoutWkt()).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), len(r2), 1)
	assert.Equal(suite.T(), polygonName, r2[0])
}

func (suite *TairGisTestSuite) TestGisSearchByMember() {
	randomStr1 := randStrGis(20)
	key := "shenzhen" + randomStr1
	polygonName := "Palermo"
	polygonName1 := "Catania"
	polygonName2 := "Agrigento"
	polygonWktText := "POINT (13.361389 38.115556)"
	polygonWktText1 := "POINT (15.087269 37.502669)"
	polygonWktText2 := "POINT (13.583333 37.316667)"

	r, e := suite.tairClient.GisAdd(ctx, key, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, int64(1))

	r1, e1 := suite.tairClient.GisAdd(ctx, key, polygonName1, polygonWktText1).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(1))

	r2, e2 := suite.tairClient.GisAdd(ctx, key, polygonName2, polygonWktText2).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, int64(1))
	//WithoutValue
	r3, e3 := suite.tairClient.GisSearchArgsByMember(ctx, key, polygonName, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithoutValue()).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), len(r3), 3)
	assert.Equal(suite.T(), polygonName, r3[0].FieldByString())
	assert.Empty(suite.T(), r3[0].ValueByString())
	assert.InDelta(suite.T(), r3[0].Distance(), 0, 1e-5)
	//WithValue
	r4, e4 := suite.tairClient.GisSearchArgsByMember(ctx, key, polygonName, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithValue()).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), len(r4), 3)
	assert.Equal(suite.T(), polygonName, r4[0].FieldByString())
	assert.Equal(suite.T(), "POINT(13.361389 38.115556)", r4[0].ValueByString())
	assert.InDelta(suite.T(), r4[0].Distance(), 0, 1e-5)
	// withdist
	r5, e5 := suite.tairClient.GisSearchArgsByMember(ctx, key, polygonName, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithDist()).Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), len(r5), 3)
	assert.Equal(suite.T(), polygonName, r5[0].FieldByString())
	assert.Equal(suite.T(), "POINT(13.361389 38.115556)", r5[0].ValueByString())
	assert.InDelta(suite.T(), r5[0].Distance(), 0.0, 1e-5)

	// SORT ASC
	r6, e6 := suite.tairClient.GisSearchArgsByMember(ctx, key, polygonName, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithDist().Asc()).Result()
	assert.NoError(suite.T(), e6)
	assert.Equal(suite.T(), len(r6), 3)
	assert.Equal(suite.T(), polygonName, r6[0].FieldByString())
	assert.Equal(suite.T(), "POINT(13.361389 38.115556)", r6[0].ValueByString())
	assert.InDelta(suite.T(), r6[0].Distance(), 0.0, 1e-5)
	// SORT DESC
	r7, e7 := suite.tairClient.GisSearchArgsByMember(ctx, key, polygonName, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithDist().Desc()).Result()
	assert.NoError(suite.T(), e7)
	assert.Equal(suite.T(), len(r7), 3)
	assert.Equal(suite.T(), polygonName1, r7[0].FieldByString())
	assert.Equal(suite.T(), "POINT(15.087269 37.502669)", r7[0].ValueByString())
	assert.InDelta(suite.T(), r7[0].Distance(), 166.2743, 1e-5)
	// COUNT 2
	r8, e8 := suite.tairClient.GisSearchArgsByMember(ctx, key, polygonName, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithDist().Desc().Count(2)).Result()
	assert.NoError(suite.T(), e8)
	assert.Equal(suite.T(), len(r8), 2)
	assert.Equal(suite.T(), polygonName1, r8[0].FieldByString())
	assert.Equal(suite.T(), "POINT(15.087269 37.502669)", r8[0].ValueByString())
	assert.InDelta(suite.T(), r8[0].Distance(), 166.2743, 1e-5)

	assert.Equal(suite.T(), polygonName2, r8[1].FieldByString())
	assert.Equal(suite.T(), "POINT(13.583333 37.316667)", r8[1].ValueByString())
	assert.InDelta(suite.T(), r8[1].Distance(), 90.9779, 1e-5)

}

func (suite *TairGisTestSuite) TestGisSearchArgs() {
	randomStr1 := randStrGis(20)
	key := "shenzhen" + randomStr1
	polygonName := "Palermo"
	polygonName1 := "Catania"
	polygonName2 := "Agrigento"
	polygonWktText := "POINT (13.361389 38.115556)"
	polygonWktText1 := "POINT (15.087269 37.502669)"
	polygonWktText2 := "POINT (13.583333 37.316667)"

	r, e := suite.tairClient.GisAdd(ctx, key, polygonName, polygonWktText).Result()
	assert.NoError(suite.T(), e)
	assert.Equal(suite.T(), r, int64(1))

	r1, e1 := suite.tairClient.GisAdd(ctx, key, polygonName1, polygonWktText1).Result()
	assert.NoError(suite.T(), e1)
	assert.Equal(suite.T(), r1, int64(1))

	r2, e2 := suite.tairClient.GisAdd(ctx, key, polygonName2, polygonWktText2).Result()
	assert.NoError(suite.T(), e2)
	assert.Equal(suite.T(), r2, int64(1))
	//WithoutValue
	r3, e3 := suite.tairClient.GisSearchArgs(ctx, key, 15, 37, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithoutValue()).Result()
	assert.NoError(suite.T(), e3)
	assert.Equal(suite.T(), len(r3), 3)
	assert.Equal(suite.T(), polygonName, r3[0].FieldByString())
	assert.Empty(suite.T(), r3[0].ValueByString())
	assert.InDelta(suite.T(), r3[0].Distance(), 0, 1e-5)

	//WithValue
	r4, e4 := suite.tairClient.GisSearchArgs(ctx, key, 15, 37, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithValue()).Result()
	assert.NoError(suite.T(), e4)
	assert.Equal(suite.T(), len(r4), 3)
	assert.Equal(suite.T(), polygonName, r4[0].FieldByString())
	assert.Equal(suite.T(), "POINT(13.361389 38.115556)", r4[0].ValueByString())
	assert.InDelta(suite.T(), r4[0].Distance(), 0, 1e-5)

	// withdist
	r5, e5 := suite.tairClient.GisSearchArgs(ctx, key, 15, 37, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithDist()).Result()
	assert.NoError(suite.T(), e5)
	assert.Equal(suite.T(), len(r5), 3)
	assert.Equal(suite.T(), polygonName, r5[0].FieldByString())
	assert.Equal(suite.T(), "POINT(13.361389 38.115556)", r5[0].ValueByString())
	assert.InDelta(suite.T(), r5[0].Distance(), 190.4424, 1e-5)

	// SORT ASC
	r6, e6 := suite.tairClient.GisSearchArgs(ctx, key, 15, 37, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithDist().Asc()).Result()
	assert.NoError(suite.T(), e6)
	assert.Equal(suite.T(), len(r6), 3)
	assert.Equal(suite.T(), polygonName1, r6[0].FieldByString())
	assert.Equal(suite.T(), "POINT(15.087269 37.502669)", r6[0].ValueByString())
	assert.InDelta(suite.T(), r6[0].Distance(), 56.4413, 1e-5)
	// SORT ASC
	r7, e7 := suite.tairClient.GisSearchArgs(ctx, key, 15, 37, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithDist().Desc()).Result()
	assert.NoError(suite.T(), e7)
	assert.Equal(suite.T(), len(r7), 3)
	assert.Equal(suite.T(), polygonName, r7[0].FieldByString())
	assert.Equal(suite.T(), "POINT(13.361389 38.115556)", r7[0].ValueByString())
	assert.InDelta(suite.T(), r7[0].Distance(), 190.4424, 1e-5)
	// COUNT 2
	r8, e8 := suite.tairClient.GisSearchArgs(ctx, key, 15, 37, 200, "km",
		tair.GisArgs{}.NewGisArgs().WithDist().Desc().Count(2)).Result()
	assert.NoError(suite.T(), e8)
	assert.Equal(suite.T(), len(r8), 2)
	assert.Equal(suite.T(), polygonName, r8[0].FieldByString())
	assert.Equal(suite.T(), "POINT(13.361389 38.115556)", r8[0].ValueByString())
	assert.InDelta(suite.T(), r8[0].Distance(), 190.4424, 1e-5)

	assert.Equal(suite.T(), polygonName2, r8[1].FieldByString())
	assert.Equal(suite.T(), "POINT(13.583333 37.316667)", r8[1].ValueByString())
	assert.InDelta(suite.T(), r8[1].Distance(), 130.4233, 1e-5)
}

func (suite *TairGisTestSuite) TestGisAddErrorType() {
	polygonName := "Palermo"
	polygonWktText := "POINT (13.361389 38.115556)"
	suite.tairClient.Set(ctx, randomGisKey, "bar", 0)
	r, e := suite.tairClient.GisAdd(ctx, randomGisKey, polygonName, polygonWktText).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
	assert.Zero(suite.T(), r)
}

func (suite *TairGisTestSuite) TestGisDelErrorType() {
	polygonName := "Palermo"
	suite.tairClient.Set(ctx, randomGisKey, "bar", 0)
	r, e := suite.tairClient.GisDel(ctx, randomGisKey, polygonName).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
	assert.Zero(suite.T(), r)
}
func (suite *TairGisTestSuite) TestGisSearchErrorType() {
	polygonName := "Palermo"
	suite.tairClient.Set(ctx, randomGisKey, "bar", 0)
	_, e := suite.tairClient.GisSearch(ctx, randomGisKey, polygonName).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairGisTestSuite) TestGisContainsErrorType() {
	polygonName := "Palermo"
	suite.tairClient.Set(ctx, randomGisKey, "bar", 0)
	_, e := suite.tairClient.GisContains(ctx, randomGisKey, polygonName).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairGisTestSuite) TestGisIntersectsErrorType() {
	polygonName := "Palermo"
	suite.tairClient.Set(ctx, randomGisKey, "bar", 0)
	_, e := suite.tairClient.GisIntersects(ctx, randomGisKey, polygonName).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func (suite *TairGisTestSuite) TestGisGetAllErrorType() {
	suite.tairClient.Set(ctx, randomGisKey, "bar", 0)
	_, e := suite.tairClient.GisGetAll(ctx, randomGisKey).Result()
	assert.Error(suite.T(), e)
	assert.Contains(suite.T(), e, "WRONGTYPE")
}

func TestTairGisTestSuite(t *testing.T) {
	suite.Run(t, new(TairGisTestSuite))
}
