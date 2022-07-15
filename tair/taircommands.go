package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type tairCmdable func(ctx context.Context, cmd redis.Cmder) error // cmdable 是一个函数接口

// TairCmdable  define all the api of tair module in the TairCmdable interface
type TairCmdable interface {
	// TairString
	Cas(ctx context.Context, key string, oldVal, newVal interface{}) *redis.IntCmd
	CasArgs(ctx context.Context, key string, oldVal, newVal interface{}, a *CasArgs) *redis.IntCmd
	Cad(ctx context.Context, key string, value interface{}) *redis.IntCmd
	ExSet(ctx context.Context, key string, value interface{}) *redis.StatusCmd
	ExSetArgs(ctx context.Context, key string, value interface{}, a *ExSetArgs) *redis.StatusCmd
	ExSetWithVersion(ctx context.Context, key string, value interface{}, a *ExSetArgs) *redis.IntCmd
	ExSetVer(ctx context.Context, key string) *redis.IntCmd
	ExGet(ctx context.Context, key string) *redis.SliceCmd
	ExGetFlags(ctx context.Context, key string) *redis.SliceCmd
	ExIncrBy(ctx context.Context, key string, incr int64) *redis.IntCmd
	ExIncrByArgs(ctx context.Context, key string, incr int64, a *ExIncrByArgs) *redis.IntCmd
	ExIncrByWithVersion(ctx context.Context, key string, incr int64, a *ExIncrByArgs) *redis.SliceCmd
	ExIncrByFloat(ctx context.Context, key string, incr float64) *redis.FloatCmd
	ExIncrByFloatArgs(ctx context.Context, key string, incr float64, a *ExIncrByArgs) *redis.FloatCmd
	ExCas(ctx context.Context, key string, newVal interface{}, version int64) *redis.SliceCmd
	ExCad(ctx context.Context, key string, version int) *redis.IntCmd
	ExAppend(ctx context.Context, key string, value interface{}, nxxx, verAbs string, version int64) *redis.IntCmd
	ExPreAppend(ctx context.Context, key string, value interface{}, nxxx, verAbs string, version int) *redis.IntCmd
	ExGae(ctx context.Context, key string, expxwithat string, time time.Duration) *redis.SliceCmd

	// TairZset
	ExZAddManyScore(ctx context.Context, key string, member string, scores ...float64) *redis.IntCmd
	ExZAdd(ctx context.Context, key string, score string, member string) *redis.IntCmd
	ExZAddArgs(ctx context.Context, key string, score string, member string, p *ExZAddArgs) *redis.IntCmd
	ExZAddManyMember(ctx context.Context, key string, member ...ExZAddMember) *redis.IntCmd
	ExZAddManyMemberArgs(ctx context.Context, key string, p *ExZAddArgs, member ...ExZAddMember) *redis.IntCmd
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
	ExZRangeByScoreWithArgs(ctx context.Context, key, min, max string, a *ExZRangeArgs) *redis.StringSliceCmd
	ExZRevRangeByScore(ctx context.Context, key, min, max string) *redis.StringSliceCmd
	ExZRevRangeByScoreWithArgs(ctx context.Context, key, min, max string, a *ExZRangeArgs) *redis.StringSliceCmd
	ExZRangeByLex(ctx context.Context, key, min, max string) *redis.StringSliceCmd
	ExZRangeByLexWithArgs(ctx context.Context, key, min, max string, a *ExZRangeArgs) *redis.StringSliceCmd
	ExZRevRangeByLex(ctx context.Context, key, min, max string) *redis.StringSliceCmd
	ExZRevRangeByLexWithArgs(ctx context.Context, key, min, max string, a *ExZRangeArgs) *redis.StringSliceCmd
	ExZCard(ctx context.Context, key string) *redis.IntCmd
	ExZRank(ctx context.Context, key, member string) *redis.IntCmd
	ExZRevRank(ctx context.Context, key, member string) *redis.IntCmd
	ExZRankByScore(ctx context.Context, key, score string) *redis.IntCmd
	ExZRevRankByScore(ctx context.Context, key, score string) *redis.IntCmd
	ExZCount(ctx context.Context, key, min, max string) *redis.IntCmd
	ExZLexCount(ctx context.Context, key, min, max string) *redis.IntCmd

	// TairHash
	ExHSet(ctx context.Context, key, field, value string) *redis.IntCmd
	ExHGet(ctx context.Context, key, field string) *redis.StringCmd
	ExHSetArgs(ctx context.Context, key, field, value string, a *ExHSetArgs) *redis.IntCmd
	ExHSetNx(ctx context.Context, key, field, value string) *redis.IntCmd
	ExHMSet(ctx context.Context, key string, fieldValue map[string]string) *redis.StatusCmd
	ExHMSetWithOpts(ctx context.Context, key string, arg ...ExHMSetWithOptsArgs) *redis.StatusCmd
	ExHPExpire(ctx context.Context, key, field string, milliseconds int) *redis.BoolCmd
	ExHPExpireAt(ctx context.Context, key, field string, unixTime int) *redis.BoolCmd
	ExHExpire(ctx context.Context, key, field string, seconds int) *redis.BoolCmd
	ExHExpireAt(ctx context.Context, key, field string, unixTime int) *redis.BoolCmd
	ExHPTTL(ctx context.Context, key, field string) *redis.IntCmd
	ExHTTL(ctx context.Context, key, field string) *redis.IntCmd
	ExHVer(ctx context.Context, key, field string) *redis.IntCmd
	ExHSetVer(ctx context.Context, key, field string, version int) *redis.BoolCmd
	ExHIncrBy(ctx context.Context, key, field string, value int) *redis.IntCmd
	ExHIncrByArgs(ctx context.Context, key, field string, value int, a *ExHIncrArgs) *redis.IntCmd
	ExHIncrByFloat(ctx context.Context, key, field string, value int) *redis.StringCmd
	ExHIncrByFloatArgs(ctx context.Context, key, field string, value int, a *ExHIncrArgs) *redis.IntCmd
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
}
