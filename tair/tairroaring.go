package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
)

func (tc tairCmdable) TrSetBit(ctx context.Context, key string, offset int64, value int64) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.setbit"
	a[1] = key
	a[2] = offset
	a[3] = value
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrSetBits(ctx context.Context, key string, fields ...int64) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.setbits"
	a[1] = key
	for _, f := range fields {
		a = append(a, f)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) TrGetBit(ctx context.Context, key string, offset int64) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "tr.getbit"
	a[1] = key
	a[2] = offset
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrGetBits(ctx context.Context, key string, fields ...int64) *redis.IntSliceCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.getbits"
	a[1] = key
	for _, f := range fields {
		a = append(a, f)
	}
	cmd := redis.NewIntSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrClearBits(ctx context.Context, key string, fields ...int64) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.clearbits"
	a[1] = key
	for _, f := range fields {
		a = append(a, f)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrRange(ctx context.Context, key string, start int64, end int64) *redis.IntSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.range"
	a[1] = key
	a[2] = start
	a[3] = end
	cmd := redis.NewIntSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrRangeBitArray(ctx context.Context, key string, start int64, end int64) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.rangebitarray"
	a[1] = key
	a[2] = start
	a[3] = end
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrAppendBitArray(ctx context.Context, key string, offset int64, value string) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.appendbitarray"
	a[1] = key
	a[2] = offset
	a[3] = value
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrSetRange(ctx context.Context, key string, start int64, end int64) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.setrange"
	a[1] = key
	a[2] = start
	a[3] = end
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrFlipRange(ctx context.Context, key string, start int64, end string) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.fliprange"
	a[1] = key
	a[2] = start
	a[3] = end
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrBitCount(ctx context.Context, key string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.bitcount"
	a[1] = key
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrBitCountRange(ctx context.Context, key string, start int64, end int64) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.bitcount"
	a[1] = key
	a[2] = start
	a[3] = end
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrMin(ctx context.Context, key string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.min"
	a[1] = key
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrMax(ctx context.Context, key string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.max"
	a[1] = key
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrOptimize(ctx context.Context, key string) *redis.StringCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.optimize"
	a[1] = key
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrStat(ctx context.Context, key string, json bool) *redis.StringCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.stat"
	a[1] = key
	if json {
		a = append(a, "JSON")
	}
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) TrBitPos(ctx context.Context, key string, value int64, count int64) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.bitpos"
	a[1] = key
	a[2] = value
	a[3] = count
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrBitPosFirst(ctx context.Context, key string, value int64) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "tr.bitpos"
	a[1] = key
	a[2] = value
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrRank(ctx context.Context, key string, offset int64) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "tr.rank"
	a[1] = key
	a[2] = offset
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrBitOp(ctx context.Context, destKey string, operation string, keys ...string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "tr.bitop"
	a[1] = destKey
	a[2] = operation
	for _, k := range keys {
		a = append(a, k)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrBitOpCard(ctx context.Context, operation string, keys ...string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.bitopcard"
	a[1] = operation
	for _, k := range keys {
		a = append(a, k)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrScan(ctx context.Context, key string, cursor int64, count int64) *redis.SliceCmd {
	a := make([]interface{}, 5)
	a[0] = "tr.scan"
	a[1] = key
	a[2] = cursor
	a[3] = "COUNT"
	a[4] = count
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrScanCount(ctx context.Context, key string, cursor int64) *redis.SliceCmd {
	a := make([]interface{}, 3)
	a[0] = "tr.scan"
	a[1] = key
	a[2] = cursor
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrDiff(ctx context.Context, destKey, key1, key2 string) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.diff"
	a[1] = destKey
	a[2] = key1
	a[3] = key2
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrSetIntArray(ctx context.Context, key string, fields ...int64) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.setintarray"
	a[1] = key
	for _, f := range fields {
		a = append(a, f)
	}
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrAppendIntArray(ctx context.Context, key string, fields ...int64) *redis.StringCmd {
	a := make([]interface{}, 2)
	a[0] = "tr.appendintarray"
	a[1] = key
	for _, f := range fields {
		a = append(a, f)
	}
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrSetBitArray(ctx context.Context, key, value string) *redis.FloatCmd {
	a := make([]interface{}, 4)
	a[0] = "tr.setbitarray"
	a[1] = key
	a[2] = value
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrJaccard(ctx context.Context, key1, key2 string) *redis.FloatCmd {
	a := make([]interface{}, 3)
	a[0] = "tr.jaccard"
	a[1] = key1
	a[2] = key2
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TrContains(ctx context.Context, key1, key2 string) *redis.BoolCmd {
	a := make([]interface{}, 3)
	a[0] = "tr.contains"
	a[1] = key1
	a[2] = key2
	cmd := redis.NewBoolCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
