package tair_test

import (
	"context"
	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type TairRoaringTestSuite struct {
	suite.Suite
	tairClient *tair.TairClient
}

func (suite *TairRoaringTestSuite) SetupTest() {
	suite.tairClient = tair.NewTairClient(redisOptions())
	assert.Equal(suite.T(), "OK", suite.tairClient.FlushDB(ctx).Val())
}

func (suite *TairRoaringTestSuite) TearDownTest() {
	assert.NoError(suite.T(), suite.tairClient.Close())
}

func (suite *TairRoaringTestSuite) TestTrSetBit() {
	result, err := suite.tairClient.TrSetBit(ctx, "foo", 10, 1).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(0))

	result2, err2 := suite.tairClient.TrSetBit(ctx, "foo", 20, 1).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, int64(0))

	result3, err3 := suite.tairClient.TrSetBit(ctx, "foo", 30, 1).Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, int64(0))

	result4, err4 := suite.tairClient.TrSetBit(ctx, "foo", 30, 0).Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), result4, int64(1))
}

func (suite *TairRoaringTestSuite) TestSetBitsBitCountClearBits() {
	result, err := suite.tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(5))

	result1, err1 := suite.tairClient.TrBitCount(ctx, "foo").Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, int64(5))

	result2, err2 := suite.tairClient.TrSetBits(ctx, "foo", 5, 7, 9, 11, 13).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, int64(7))

	result5, err5 := suite.tairClient.TrBitCount(ctx, "foo").Result()
	assert.NoError(suite.T(), err5)
	assert.Equal(suite.T(), result5, int64(7))

	result3, err3 := suite.tairClient.TrClearBits(ctx, "foo", 5, 6, 7, 8, 9).Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, int64(3))

	result7, err7 := suite.tairClient.TrBitCount(ctx, "foo").Result()
	assert.NoError(suite.T(), err7)
	assert.Equal(suite.T(), result7, int64(4))

	result8, err8 := suite.tairClient.TrGetBits(ctx, "foo", 1, 2, 3, 4, 5).Result()
	assert.NoError(suite.T(), err8)
	assert.Equal(suite.T(), result8, []int64{1, 0, 1, 0, 0})
}

func (suite *TairRoaringTestSuite) TestTrSetBitsTaRange() {
	result, err := suite.tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(5))

	result1, err1 := suite.tairClient.TrRange(ctx, "foo", 1, 5).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, []int64{1, 3, 5})

	result3, err3 := suite.tairClient.TrRange(ctx, "foo", 0, 4).Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, []int64{1, 3})
}

func (suite *TairRoaringTestSuite) TestTrRange() {
	suite.tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9)
	result, err := suite.tairClient.TrRange(ctx, "foo", 1, 5).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, []int64{1, 3, 5})
	result1, err1 := suite.tairClient.TrRange(ctx, "foo", 0, 4).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, []int64{1, 3})
}

func (suite *TairRoaringTestSuite) TestAppendBitArray() {
	result, err := suite.tairClient.TrAppendBitArray(ctx, "foo", 0, "101010101").Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(5))

	result1, err1 := suite.tairClient.TrRange(ctx, "foo", 0, 10).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, []int64{1, 3, 5, 7, 9})

	suite.tairClient.Del(ctx, "foo")
	result2, err2 := suite.tairClient.TrAppendBitArray(ctx, "foo", -1, "101010101").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, int64(5))
	result3, err3 := suite.tairClient.TrRange(ctx, "foo", 0, 10).Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, []int64{0, 2, 4, 6, 8})
}

func (suite *TairRoaringTestSuite) TestScanCount() {
	result1, err1 := suite.tairClient.TrScan(ctx, "no-key", 0).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1[0], int64(0))
	assert.Equal(suite.T(), result1[1], make([]interface{}, 0))

	result2, err2 := suite.tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, int64(5))

	result3, err3 := suite.tairClient.TrScan(ctx, "foo", 0).Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3[0], int64(0))
	assert.Equal(suite.T(), result3[1], append(make([]interface{}, 0), int64(1), int64(3), int64(5), int64(7), int64(9)))

	result4, err4 := suite.tairClient.TrScanCount(ctx, "foo", 4, 2).Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), result4[0], int64(9))
	assert.Equal(suite.T(), result4[1], append(make([]interface{}, 0), int64(5), int64(7)))
}

func (suite *TairRoaringTestSuite) TestStatus() {
	result, err := suite.tairClient.TrSetBits(ctx, "foo", 1, 2, 3, 4, 5, 6, 7, 8, 9).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(9))

	result2, err2 := suite.tairClient.TrOptimize(ctx, "foo").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, "OK")

	result3, err3 := suite.tairClient.TrBitCount(ctx, "foo").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, int64(9))

	result4, err4 := suite.tairClient.TrBitCountRange(ctx, "foo", 0, 5).Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), result4, int64(5))

	result5, err5 := suite.tairClient.TrBitCountRange(ctx, "foo", 9, 20).Result()
	assert.NoError(suite.T(), err5)
	assert.Equal(suite.T(), result5, int64(1))

	result6, err6 := suite.tairClient.TrBitPos(ctx, "foo", 1).Result()
	assert.NoError(suite.T(), err6)
	assert.Equal(suite.T(), result6, int64(1))

	result7, err7 := suite.tairClient.TrBitPos(ctx, "foo", 1).Result()
	assert.NoError(suite.T(), err7)
	assert.Equal(suite.T(), result7, int64(1))

	result8, err8 := suite.tairClient.TrBitPosCount(ctx, "foo", 1, 2).Result()
	assert.NoError(suite.T(), err8)
	assert.Equal(suite.T(), result8, int64(2))

	result9, err9 := suite.tairClient.TrBitPosCount(ctx, "foo", 1, -4).Result()
	assert.NoError(suite.T(), err9)
	assert.Equal(suite.T(), result9, int64(6))

	result10, err10 := suite.tairClient.TrBitPosCount(ctx, "foo", 0, 1).Result()
	assert.NoError(suite.T(), err10)
	assert.Equal(suite.T(), result10, int64(0))

	result11, err11 := suite.tairClient.TrStat(ctx, "foo", false).Result()
	assert.NoError(suite.T(), err11)
	assert.Equal(suite.T(), result11, "cardinality: 9\r\n"+
		"number of containers: 1\r\n"+
		"max value: 9\r\n"+
		"min value: 1\r\n"+
		"sum value: 45\r\n"+
		"number of array containers: 0\r\n"+
		"\tarray container values: 0\r\n"+
		"\tarray container bytes: 0\r\n"+
		"number of bitset containers: 0\r\n"+
		"\tbitset container values: 0\r\n"+
		"\tbitset container bytes: 0\r\n"+
		"number of run containers: 1\r\n"+
		"\trun container values: 9\r\n"+
		"\trun container bytes: 6\r\n")
}

func (suite *TairRoaringTestSuite) TestEmptyKey() {
	result, err := suite.tairClient.TrRange(ctx, "foo", 0, 4).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, []int64{})

	result1, err1 := suite.tairClient.TrMin(ctx, "foo").Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, int64(-1))

	result2, err2 := suite.tairClient.TrMax(ctx, "foo").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, int64(-1))

	result3, err3 := suite.tairClient.TrRank(ctx, "foo", 1).Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, int64(-1))

	_, err4 := suite.tairClient.TrStat(ctx, "foo", false).Result()
	assert.Equal(suite.T(), err4, redis.Nil)

	_, err5 := suite.tairClient.TrOptimize(ctx, "foo").Result()
	assert.Equal(suite.T(), err5, redis.Nil)

	result6, err6 := suite.tairClient.TrBitCount(ctx, "foo").Result()
	assert.NoError(suite.T(), err6)
	assert.Equal(suite.T(), result6, int64(0))

	result7, err7 := suite.tairClient.TrClearBits(ctx, "foo", 1, 3, 5).Result()
	assert.NoError(suite.T(), err7)
	assert.Equal(suite.T(), result7, int64(0))
}

func (suite *TairRoaringTestSuite) TestBitOpTest() {
	result, err := suite.tairClient.TrAppendIntArray(ctx, "foo", 1, 3, 5, 7, 9).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, "OK")

	result1, err1 := suite.tairClient.TrAppendIntArray(ctx, "bar", 2, 4, 6, 8, 10).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, "OK")

	result2, err2 := suite.tairClient.TrBitOp(ctx, "dest", "OR", "foo", "bar").Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, int64(10))

	result3, err3 := suite.tairClient.TrBitOpCard(ctx, "AND", "foo", "bar").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, int64(0))
}

func (suite *TairRoaringTestSuite) TestGetMany() {
	result, err := suite.tairClient.TrAppendIntArray(ctx, "foo", 1, 3, 5, 7, 9, 11, 13, 15, 17, 19).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, "OK")

	result1, err1 := suite.tairClient.TrRange(ctx, "foo", 0, 4).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, []int64{1, 3})
}

func (suite *TairRoaringTestSuite) TestMultiKey() {
	result, err := suite.tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9).Result()
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), result, int64(5))

	result1, err1 := suite.tairClient.TrSetBits(ctx, "bar", 2, 4, 6, 8, 10).Result()
	assert.NoError(suite.T(), err1)
	assert.Equal(suite.T(), result1, int64(5))

	result2, err2 := suite.tairClient.TrSetRange(ctx, "baz", 1, 10).Result()
	assert.NoError(suite.T(), err2)
	assert.Equal(suite.T(), result2, int64(10))

	result3, err3 := suite.tairClient.TrContains(ctx, "foo", "bar").Result()
	assert.NoError(suite.T(), err3)
	assert.Equal(suite.T(), result3, false)

	result4, err4 := suite.tairClient.TrContains(ctx, "foo", "baz").Result()
	assert.NoError(suite.T(), err4)
	assert.Equal(suite.T(), result4, true)

	result5, err5 := suite.tairClient.TrJaccard(ctx, "foo", "baz").Result()
	assert.NoError(suite.T(), err5)
	assert.Equal(suite.T(), result5, 0.5)

	result6, err6 := suite.tairClient.TrDiff(ctx, "result", "foo", "bar").Result()
	assert.NoError(suite.T(), err6)
	assert.Equal(suite.T(), result6, "OK")
}

func TestTairRoaringTestSuite(t *testing.T) {
	suite.Run(t, new(TairRoaringTestSuite))
}

var _ = Describe("tair roaring commands", func() {
	ctx := context.TODO()
	var tairClient *tair.TairClient
	BeforeEach(func() {
		tairClient = tair.NewTairClient(redisOptions())
		Expect(tairClient.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(tairClient.Close()).NotTo(HaveOccurred())
	})
	It("TrSetBit", func() {
		result, err := tairClient.TrSetBit(ctx, "foo", 10, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(0)))

		result2, err2 := tairClient.TrSetBit(ctx, "foo", 20, 1).Result()
		Expect(err2).NotTo(HaveOccurred())
		Expect(result2).To(Equal(int64(0)))

		result3, err3 := tairClient.TrSetBit(ctx, "foo", 30, 1).Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3).To(Equal(int64(0)))

		result4, err4 := tairClient.TrSetBit(ctx, "foo", 30, 0).Result()
		Expect(err4).NotTo(HaveOccurred())
		Expect(result4).To(Equal(int64(1)))
	})

	It("TrSetBits TrBitCount TrClearBits", func() {
		result, err := tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(5)))

		result1, err1 := tairClient.TrBitCount(ctx, "foo").Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(result1).To(Equal(int64(5)))

		result2, err2 := tairClient.TrSetBits(ctx, "foo", 5, 7, 9, 11, 13).Result()
		Expect(err2).NotTo(HaveOccurred())
		Expect(result2).To(Equal(int64(7)))

		result5, err5 := tairClient.TrBitCount(ctx, "foo").Result()
		Expect(err5).NotTo(HaveOccurred())
		Expect(result5).To(Equal(int64(7)))

		result3, err3 := tairClient.TrClearBits(ctx, "foo", 5, 6, 7, 8, 9).Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3).To(Equal(int64(3)))

		result7, err7 := tairClient.TrBitCount(ctx, "foo").Result()
		Expect(err7).NotTo(HaveOccurred())
		Expect(result7).To(Equal(int64(4)))

		result8, err8 := tairClient.TrGetBits(ctx, "foo", 1, 2, 3, 4, 5).Result()
		Expect(err8).NotTo(HaveOccurred())
		Expect(result8).To(Equal([]int64{1, 0, 1, 0, 0}))
	})

	It("TrSetBits TrBitCount TrClearBits", func() {
		result, err := tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(5)))

		result1, err1 := tairClient.TrRange(ctx, "foo", 1, 5).Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(result1).To(Equal([]int64{1, 3, 5}))

		result3, err3 := tairClient.TrRange(ctx, "foo", 0, 4).Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3).To(Equal([]int64{1, 3}))
	})

	It("TrRange", func() {
		tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9)
		result, err := tairClient.TrRange(ctx, "foo", 1, 5).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal([]int64{1, 3, 5}))
		result1, err1 := tairClient.TrRange(ctx, "foo", 0, 4).Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(result1).To(Equal([]int64{1, 3}))
	})

	It("TrAppendBitArray", func() {
		result, err := tairClient.TrAppendBitArray(ctx, "foo", 0, "101010101").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(5)))

		result1, err1 := tairClient.TrRange(ctx, "foo", 0, 10).Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(result1).To(Equal([]int64{1, 3, 5, 7, 9}))

		tairClient.Del(ctx, "foo")
		result2, err2 := tairClient.TrAppendBitArray(ctx, "foo", -1, "101010101").Result()
		Expect(err2).NotTo(HaveOccurred())
		Expect(result2).To(Equal(int64(5)))
		result3, err3 := tairClient.TrRange(ctx, "foo", 0, 10).Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3).To(Equal([]int64{0, 2, 4, 6, 8}))
	})

	It("TrScanCount", func() {
		result1, err1 := tairClient.TrScan(ctx, "no-key", 0).Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(result1[0]).To(Equal(int64(0)))
		Expect(result1[1]).To(Equal(make([]interface{}, 0)))

		result2, err2 := tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9).Result()
		Expect(err2).NotTo(HaveOccurred())
		Expect(result2).To(Equal(int64(5)))

		result3, err3 := tairClient.TrScan(ctx, "foo", 0).Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3[0]).To(Equal(int64(0)))
		Expect(result3[1]).To(Equal(append(make([]interface{}, 0), int64(1), int64(3), int64(5), int64(7), int64(9))))

		result4, err4 := tairClient.TrScanCount(ctx, "foo", 4, 2).Result()
		Expect(err4).NotTo(HaveOccurred())
		Expect(result4[0]).To(Equal(int64(9)))
		Expect(result4[1]).To(Equal(append(make([]interface{}, 0), int64(5), int64(7))))
	})

	It("TrStatus", func() {
		result, err := tairClient.TrSetBits(ctx, "foo", 1, 2, 3, 4, 5, 6, 7, 8, 9).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(9)))

		result2, err2 := tairClient.TrOptimize(ctx, "foo").Result()
		Expect(err2).NotTo(HaveOccurred())
		Expect(result2).To(Equal("OK"))

		result3, err3 := tairClient.TrBitCount(ctx, "foo").Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3).To(Equal(int64(9)))

		result4, err4 := tairClient.TrBitCountRange(ctx, "foo", 0, 5).Result()
		Expect(err4).NotTo(HaveOccurred())
		Expect(result4).To(Equal(int64(5)))

		result5, err5 := tairClient.TrBitCountRange(ctx, "foo", 9, 20).Result()
		Expect(err5).NotTo(HaveOccurred())
		Expect(result5).To(Equal(int64(1)))

		result6, err6 := tairClient.TrBitPos(ctx, "foo", 1).Result()
		Expect(err6).NotTo(HaveOccurred())
		Expect(result6).To(Equal(int64(1)))

		result7, err7 := tairClient.TrBitPos(ctx, "foo", 1).Result()
		Expect(err7).NotTo(HaveOccurred())
		Expect(result7).To(Equal(int64(1)))

		result8, err8 := tairClient.TrBitPosCount(ctx, "foo", 1, 2).Result()
		Expect(err8).NotTo(HaveOccurred())
		Expect(result8).To(Equal(int64(2)))

		result9, err9 := tairClient.TrBitPosCount(ctx, "foo", 1, -4).Result()
		Expect(err9).NotTo(HaveOccurred())
		Expect(result9).To(Equal(int64(6)))

		result10, err10 := tairClient.TrBitPosCount(ctx, "foo", 0, 1).Result()
		Expect(err10).NotTo(HaveOccurred())
		Expect(result10).To(Equal(int64(0)))

		result11, err11 := tairClient.TrStat(ctx, "foo", false).Result()
		Expect(err11).NotTo(HaveOccurred())
		Expect(result11).To(Equal("cardinality: 9\r\n" +
			"number of containers: 1\r\n" +
			"max value: 9\r\n" +
			"min value: 1\r\n" +
			"sum value: 45\r\n" +
			"number of array containers: 0\r\n" +
			"\tarray container values: 0\r\n" +
			"\tarray container bytes: 0\r\n" +
			"number of bitset containers: 0\r\n" +
			"\tbitset container values: 0\r\n" +
			"\tbitset container bytes: 0\r\n" +
			"number of run containers: 1\r\n" +
			"\trun container values: 9\r\n" +
			"\trun container bytes: 6\r\n"))
	})

	It("Tr Empty Key", func() {
		result, err := tairClient.TrRange(ctx, "foo", 0, 4).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal([]int64{}))

		result1, err1 := tairClient.TrMin(ctx, "foo").Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(result1).To(Equal(int64(-1)))

		result2, err2 := tairClient.TrMax(ctx, "foo").Result()
		Expect(err2).NotTo(HaveOccurred())
		Expect(result2).To(Equal(int64(-1)))

		result3, err3 := tairClient.TrRank(ctx, "foo", 1).Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3).To(Equal(int64(-1)))

		_, err4 := tairClient.TrStat(ctx, "foo", false).Result()
		Expect(err4).To(Equal(redis.Nil))
		_, err5 := tairClient.TrOptimize(ctx, "foo").Result()
		Expect(err5).To(Equal(redis.Nil))

		result6, err6 := tairClient.TrBitCount(ctx, "foo").Result()
		Expect(err6).NotTo(HaveOccurred())
		Expect(result6).To(Equal(int64(0)))

		result7, err7 := tairClient.TrClearBits(ctx, "foo", 1, 3, 5).Result()
		Expect(err7).NotTo(HaveOccurred())
		Expect(result7).To(Equal(int64(0)))
	})

	It("Tr Bit Op test", func() {
		result, err := tairClient.TrAppendIntArray(ctx, "foo", 1, 3, 5, 7, 9).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("OK"))

		result1, err1 := tairClient.TrAppendIntArray(ctx, "bar", 2, 4, 6, 8, 10).Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(result1).To(Equal("OK"))

		result2, err2 := tairClient.TrBitOp(ctx, "dest", "OR", "foo", "bar").Result()
		Expect(err2).NotTo(HaveOccurred())
		Expect(result2).To(Equal(int64(10)))

		result3, err3 := tairClient.TrBitOpCard(ctx, "AND", "foo", "bar").Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3).To(Equal(int64(0)))
	})

	It("Tr Get Many test", func() {
		result, err := tairClient.TrAppendIntArray(ctx, "foo", 1, 3, 5, 7, 9, 11, 13, 15, 17, 19).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("OK"))

		result1, err1 := tairClient.TrRange(ctx, "foo", 0, 4).Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(result1).To(Equal([]int64{1, 3}))
	})

	It("Tr Multi Key test", func() {
		result, err := tairClient.TrSetBits(ctx, "foo", 1, 3, 5, 7, 9).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(int64(5)))

		result1, err1 := tairClient.TrSetBits(ctx, "bar", 2, 4, 6, 8, 10).Result()
		Expect(err1).NotTo(HaveOccurred())
		Expect(result1).To(Equal(int64(5)))

		result2, err2 := tairClient.TrSetRange(ctx, "baz", 1, 10).Result()
		Expect(err2).NotTo(HaveOccurred())
		Expect(result2).To(Equal(int64(10)))

		result3, err3 := tairClient.TrContains(ctx, "foo", "bar").Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3).To(Equal(false))

		result4, err4 := tairClient.TrContains(ctx, "foo", "baz").Result()
		Expect(err4).NotTo(HaveOccurred())
		Expect(result4).To(Equal(true))

		result5, err5 := tairClient.TrJaccard(ctx, "foo", "baz").Result()
		Expect(err5).NotTo(HaveOccurred())
		Expect(result5).To(Equal(0.5))

		result6, err6 := tairClient.TrDiff(ctx, "result", "foo", "bar").Result()
		Expect(err6).NotTo(HaveOccurred())
		Expect(result6).To(Equal("OK"))

	})
})
