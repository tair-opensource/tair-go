package tair

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type tairCmdable func(ctx context.Context, cmd redis.Cmder) error

// TairCmdable  define all the api of tair module in the TairCmdable interface
type TairCmdable interface {
	// TairZset
	exZAdd(ctx context.Context, key string, p *ExZAddArgs, member ...ExZAddMember) *redis.IntCmd
	ExZAddManyScore(ctx context.Context, key string, member string, scores ...float64) *redis.IntCmd
	ExZAdd(ctx context.Context, key string, score string, member string) *redis.IntCmd
	ExZAddArgs(ctx context.Context, key string, score string, member string, a *ExZAddArgs) *redis.IntCmd
	ExZAddManyMember(ctx context.Context, key string, member ...ExZAddMember) *redis.IntCmd
	ExZAddManyMemberArgs(ctx context.Context, key string, a *ExZAddArgs, member ...ExZAddMember) *redis.IntCmd
	ExZIncrBy(ctx context.Context, key string, score string, member string) *redis.StringCmd
	ExZIncrByManyScore(ctx context.Context, key string, member string, score ...float64) *redis.StringSliceCmd
	ExZRem(ctx context.Context, key string, member ...string) *redis.IntCmd
	ExZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd
	ExZRemRangeByRank(ctx context.Context, key string, start, stop int) *redis.IntCmd
	ExZRemRangeByLex(ctx context.Context, key, min, max string) *redis.IntCmd
	ExZScore(ctx context.Context, key, member string) *redis.StringCmd
	ExZRange(ctx context.Context, key string, min, max int64) *redis.StringSliceCmd
	ExZRangeWithScores(ctx context.Context, key string, min, max int64) *redis.StringSliceCmd
	ExZRevRange(ctx context.Context, key string, min, max int) *redis.StringSliceCmd
	ExZRevRangeWithScores(ctx context.Context, key string, min, max int64) *redis.StringSliceCmd
	ExZRangeByScore(ctx context.Context, key, min, max string) *redis.StringSliceCmd
	ExZRangeByScoreWithArgs(ctx context.Context, key, min, max string, arg *ExZRangeArgs) *redis.StringSliceCmd
	ExZRevRangeByScore(ctx context.Context, key, min, max string) *redis.StringSliceCmd
	ExZRevRangeByScoreWithArgs(ctx context.Context, key, min, max string, arg *ExZRangeArgs) *redis.StringSliceCmd
	ExZRangeByLex(ctx context.Context, key, min, max string) *redis.StringSliceCmd
	ExZRangeByLexWithArgs(ctx context.Context, key, min, max string, args *ExZRangeArgs) *redis.StringSliceCmd
	ExZRevRangeByLex(ctx context.Context, key, min, max string) *redis.StringSliceCmd
	ExZRevRangeByLexWithArgs(ctx context.Context, key, min, max string, arg *ExZRangeArgs) *redis.StringSliceCmd
	ExZCard(ctx context.Context, key string) *redis.IntCmd
	ExZRank(ctx context.Context, key, member string) *redis.IntCmd
	ExZRevRank(ctx context.Context, key, member string) *redis.IntCmd
	ExZRankByScore(ctx context.Context, key, score string) *redis.IntCmd
	ExZRevRankByScore(ctx context.Context, key, score string) *redis.IntCmd
	ExZCount(ctx context.Context, key, min, max string) *redis.IntCmd
	ExZLexCount(ctx context.Context, key, min, max string) *redis.IntCmd
	// TairString
	Cas(ctx context.Context, key string, oldVal, newVal interface{}) *redis.IntCmd
	CasArgs(ctx context.Context, key string, oldVal, newVal interface{}, a *CasArgs) *redis.IntCmd
	Cad(ctx context.Context, key string, value interface{}) *redis.IntCmd
	ExSet(ctx context.Context, key string, value interface{}) *redis.StatusCmd
	ExSetArgs(ctx context.Context, key string, value interface{}, a *ExSetArgs) *redis.StatusCmd
	ExSetWithVersion(ctx context.Context, key string, value interface{}, exSetParam ExSetArgs) *redis.IntCmd
	ExSetVer(ctx context.Context, key string, version int64) *redis.IntCmd
	ExGet(ctx context.Context, key string) *redis.SliceCmd
	ExGetWithFlags(ctx context.Context, key string) *redis.SliceCmd
	ExIncrBy(ctx context.Context, key string, incr int64) *redis.IntCmd
	ExIncrByArgs(ctx context.Context, key string, incr int64, a *ExIncrByArgs) *redis.IntCmd
	ExIncrByWithVersion(ctx context.Context, key string, incr int64, param ExIncrByArgs) *redis.SliceCmd
	ExIncrByFloat(ctx context.Context, key string, incr float64) *redis.FloatCmd
	ExIncrByFloatArgs(ctx context.Context, key string, incr float64, a *ExIncrByArgs) *redis.FloatCmd
	ExCas(ctx context.Context, key string, newVal interface{}, version int64) *redis.SliceCmd
	ExCad(ctx context.Context, key string, version int) *redis.IntCmd
	ExAppend(ctx context.Context, key string, value interface{}, nxxx, verAbs string, version int64) *redis.IntCmd
	ExPreAppend(ctx context.Context, key string, value interface{}, nxxx, verAbs string, version int) *redis.IntCmd
	ExGae(ctx context.Context, key string, expxwithat string, time time.Duration) *redis.SliceCmd
	// TairSearch
	TftMappingIndex(ctx context.Context, index, request string) *redis.StringCmd
	TftCreateIndex(ctx context.Context, index, request string) *redis.StringCmd
	TftUpdateIndex(ctx context.Context, index, request string) *redis.StringCmd
	TftGetIndexMappings(ctx context.Context, index string) *redis.StringCmd
	TftGetIndex(ctx context.Context, index string) *redis.StringCmd
	TftGetIndexArgs(ctx context.Context, index string, args *TftIndexArgs) *redis.StringCmd
	TftAddDoc(ctx context.Context, index string, request string) *redis.StringCmd
	TftAddDocWithId(ctx context.Context, index string, request string, docId string) *redis.StringCmd
	TftMAddDoc(ctx context.Context, index string, docs map[string]string) *redis.StringCmd
	TftUpdateDocField(ctx context.Context, index, docId, docContent string) *redis.StringCmd
	TftIncrLongDocField(ctx context.Context, index, docId, docContent string, value int64) *redis.IntCmd
	TftIncrFloatDocField(ctx context.Context, index, docId, docContent string, value float64) *redis.FloatCmd
	TftDelDocField(ctx context.Context, index, docId string, field ...string) *redis.IntCmd
	TftGetDoc(ctx context.Context, index, docId string) *redis.StringCmd
	TftGetDocWithFilter(ctx context.Context, index, docId, request string) *redis.StringCmd
	TftDelDoc(ctx context.Context, index string, docId ...string) *redis.StringCmd
	TftDelAll(ctx context.Context, index string) *redis.StringCmd
	TftSearch(ctx context.Context, index string, request string) *redis.StringCmd
	TftSearchUseCache(ctx context.Context, index string, request string, useCache bool) *redis.StringCmd
	TftMSearch(ctx context.Context, indexCount int64, request string, index ...string) *redis.StringCmd
	TftExists(ctx context.Context, index string, docId string) *redis.IntCmd
	TftDocNum(ctx context.Context, index string) *redis.IntCmd
	TftScanDocId(ctx context.Context, index string, cursor string) *redis.SliceCmd
	TftScanDocIdArgs(ctx context.Context, index string, cursor string, a *TftScanArgs) *redis.SliceCmd
	TftAddSug(ctx context.Context, index string, textWeight map[string]int64) *redis.IntCmd
	TftDelSug(ctx context.Context, index string, text ...string) *redis.IntCmd
	TftSugSum(ctx context.Context, index string) *redis.IntCmd
	TftGetSug(ctx context.Context, index string, prefix string, count int8, fuzzy bool) *redis.StringSliceCmd
	TftGetAllSug(ctx context.Context, index string) *redis.StringSliceCmd
	// TairHash
	ExHSet(ctx context.Context, key, field, value string) *redis.IntCmd
	ExHGet(ctx context.Context, key, field string) *redis.StringCmd
	ExHSetArgs(ctx context.Context, key, field, value string, arg *ExHSetArgs) *redis.IntCmd
	ExHSetNx(ctx context.Context, key, field, value string) *redis.IntCmd
	ExHMSet(ctx context.Context, key string, fieldValue map[string]string) *redis.StatusCmd
	ExHMSetWithOpts(ctx context.Context, key string, arg ...*ExHMSetWithOptsArgs) *redis.StatusCmd
	ExHPExpire(ctx context.Context, key, field string, milliseconds int) *redis.BoolCmd
	ExHPExpireAt(ctx context.Context, key, field string, unixTime int) *redis.BoolCmd
	ExHExpire(ctx context.Context, key, field string, milliseconds int) *redis.BoolCmd
	ExHExpireAt(ctx context.Context, key, field string, unixTime int) *redis.BoolCmd
	ExHPTTL(ctx context.Context, key, field string) *redis.IntCmd
	ExHTTL(ctx context.Context, key, field string) *redis.IntCmd
	ExHVer(ctx context.Context, key, field string) *redis.IntCmd
	ExHSetVer(ctx context.Context, key, field string, version int) *redis.BoolCmd
	ExHIncrBy(ctx context.Context, key, field string, value int) *redis.IntCmd
	ExHIncrByArgs(ctx context.Context, key, field string, value int, arg *ExHIncrArgs) *redis.IntCmd
	ExHIncrByFloat(ctx context.Context, key, field string, value float64) *redis.StringCmd
	ExHIncrByFloatArgs(ctx context.Context, key, field string, value float64, arg *ExHIncrArgs) *redis.StringCmd
	ExHGetWithVer(ctx context.Context, key, field string) *redis.SliceCmd
	ExHMGet(ctx context.Context, key string, field ...string) *redis.StringSliceCmd
	ExHMGetWithVer(ctx context.Context, key string, field ...string) *redis.SliceCmd
	ExHDel(ctx context.Context, key string, field ...string) *redis.IntCmd
	ExHLen(ctx context.Context, key string) *redis.IntCmd
	ExHExists(ctx context.Context, key, field string) *redis.BoolCmd
	ExHStrLen(ctx context.Context, key, field string) *redis.IntCmd
	ExHKeys(ctx context.Context, key string) *redis.StringSliceCmd
	ExHVals(ctx context.Context, key string) *redis.StringSliceCmd
	ExHGetAll(ctx context.Context, key string) *redis.StringStringMapCmd
	ExHScan(ctx context.Context, key string, cursor string) *redis.SliceCmd
	// TairRoaring
	TrSetBit(ctx context.Context, key string, offset int64, value int64) *redis.IntCmd
	TrSetBits(ctx context.Context, key string, fields ...int64) *redis.IntCmd
	TrGetBit(ctx context.Context, key string, offset int64) *redis.IntCmd
	TrGetBits(ctx context.Context, key string, fields ...int64) *redis.IntSliceCmd
	TrClearBits(ctx context.Context, key string, fields ...int64) *redis.IntCmd
	TrRange(ctx context.Context, key string, start int64, end int64) *redis.IntSliceCmd
	TrRangeBitArray(ctx context.Context, key string, start int64, end int64) *redis.StringCmd
	TrAppendBitArray(ctx context.Context, key string, offset int64, value string) *redis.IntCmd
	TrSetRange(ctx context.Context, key string, start int64, end int64) *redis.IntCmd
	TrFlipRange(ctx context.Context, key string, start int64, end string) *redis.IntCmd
	TrBitCount(ctx context.Context, key string) *redis.IntCmd
	TrBitCountRange(ctx context.Context, key string, start int64, end int64) *redis.IntCmd
	TrMin(ctx context.Context, key string) *redis.IntCmd
	TrMax(ctx context.Context, key string) *redis.IntCmd
	TrOptimize(ctx context.Context, key string) *redis.StringCmd
	TrStat(ctx context.Context, key string, json bool) *redis.StringCmd
	TrBitPosCount(ctx context.Context, key string, value int64, count int64) *redis.IntCmd
	TrBitPos(ctx context.Context, key string, value int64) *redis.IntCmd
	TrRank(ctx context.Context, key string, offset int64) *redis.IntCmd
	TrBitOp(ctx context.Context, destKey string, operation string, keys ...string) *redis.IntCmd
	TrBitOpCard(ctx context.Context, operation string, keys ...string) *redis.IntCmd
	TrScanCount(ctx context.Context, key string, cursor int64, count int64) *redis.SliceCmd
	TrScan(ctx context.Context, key string, cursor int64) *redis.SliceCmd
	TrDiff(ctx context.Context, destKey, key1, key2 string) *redis.StringCmd
	TrSetIntArray(ctx context.Context, key string, fields ...int64) *redis.StringCmd
	TrAppendIntArray(ctx context.Context, key string, fields ...int64) *redis.StringCmd
	TrSetBitArray(ctx context.Context, key, value string) *redis.FloatCmd
	TrJaccard(ctx context.Context, key1, key2 string) *redis.FloatCmd
	TrContains(ctx context.Context, key1, key2 string) *redis.BoolCmd
	// TairBloom
	BfReserve(ctx context.Context, key string, initCapacity int64, errorRate float64) *redis.StringCmd
	BfAdd(ctx context.Context, key string, item string) *redis.BoolCmd
	BfMAdd(ctx context.Context, key string, items ...string) *redis.BoolSliceCmd
	BfExists(ctx context.Context, key string, item string) *redis.BoolCmd
	BfMExists(ctx context.Context, key string, items ...string) *redis.BoolSliceCmd
	BfInsert(ctx context.Context, key string, bfInsertArgs *BfInsertArgs, items ...string) *redis.BoolSliceCmd
	BfDebug(ctx context.Context, key string) *redis.StringSliceCmd
}

func toMs(dur time.Duration) int64 {
	if dur > 0 && dur < time.Millisecond {
		return 1
	}
	return int64(dur / time.Millisecond)
}

func toSec(dur time.Duration) int64 {
	if dur > 0 && dur < time.Second {
		return 1
	}
	return int64(dur / time.Second)
}
