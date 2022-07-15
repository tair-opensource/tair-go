package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type ExHSetArgs struct {
	arg

	xx   string
	nx   string
	ex   time.Duration
	px   time.Duration
	exAt time.Time
	pxAt time.Time
	ver  int64
	abs  int64
}

func (a ExHSetArgs) New() *ExHSetArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExHSetArgs) Xx() *ExHSetArgs {
	a.Set[XX] = true
	return a
}

func (a *ExHSetArgs) Nx() *ExHSetArgs {
	a.Set[NX] = true
	return a
}

func (a *ExHSetArgs) Ex(ex time.Duration) *ExHSetArgs {
	a.Set[EX] = true
	a.ex = ex
	return a
}

func (a *ExHSetArgs) Px(px time.Duration) *ExHSetArgs {
	a.Set[PX] = true
	a.px = px
	return a
}

func (a *ExHSetArgs) ExAt(exAt time.Time) *ExHSetArgs {
	a.Set[EXAT] = true
	a.exAt = exAt
	return a
}

func (a *ExHSetArgs) PxAt(pxAt time.Time) *ExHSetArgs {
	a.Set[PXAT] = true
	a.pxAt = pxAt
	return a
}

func (a *ExHSetArgs) Ver(ver int64) *ExHSetArgs {
	a.Set[VER] = true
	a.ver = ver
	return a
}

func (a *ExHSetArgs) Abs(abs int64) *ExHSetArgs {
	a.Set[ABS] = true
	a.abs = abs
	return a
}

func (a *ExHSetArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[XX]; ok {
		args = append(args, XX)
	}
	if _, ok := a.Set[NX]; ok {
		args = append(args, NX)
	}
	if _, ok := a.Set[EX]; ok {
		args = append(args, EX, a.ex)
	}
	if _, ok := a.Set[PX]; ok {
		args = append(args, PX, a.px)
	}
	if _, ok := a.Set[EXAT]; ok {
		args = append(args, EXAT, a.exAt.Unix())
	}
	if _, ok := a.Set[PXAT]; ok {
		args = append(args, PXAT, a.pxAt.Unix())
	}
	if _, ok := a.Set[VER]; ok {
		args = append(args, VER, a.ver)
	}
	if _, ok := a.Set[ABS]; ok {
		args = append(args, ABS, a.abs)
	}
	return args
}

type ExHMSetWithOptsArgs struct {
	arg

	field interface{}
	value interface{}
	ver   int64
	exp   int64
}

func (a ExHMSetWithOptsArgs) New() *ExHMSetWithOptsArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExHMSetWithOptsArgs) Field(field interface{}) *ExHMSetWithOptsArgs {
	a.field = field
	return a
}

func (a *ExHMSetWithOptsArgs) Value(value interface{}) *ExHMSetWithOptsArgs {
	a.value = value
	return a
}

func (a *ExHMSetWithOptsArgs) SetVer(ver int64) *ExHMSetWithOptsArgs {
	a.ver = ver
	return a
}

func (a *ExHMSetWithOptsArgs) SetExp(exp int64) *ExHMSetWithOptsArgs {
	a.exp = exp
	return a
}

func (a *ExHMSetWithOptsArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	args = append(args, a.field)
	args = append(args, a.value)
	args = append(args, a.ver)
	args = append(args, a.exp)
	return args
}

type ExHIncrArgs struct {
	arg

	ex      time.Duration
	px      time.Duration
	exAt    time.Time
	pxAt    time.Time
	ver     int64
	abs     int64
	min     int64
	max     int64
	gt      int64
	keepttl string
}

func (a ExHIncrArgs) New() *ExHIncrArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExHIncrArgs) Ex(ex time.Duration) *ExHIncrArgs {
	a.Set[EX] = true
	a.ex = ex
	return a
}

func (a *ExHIncrArgs) Px(px time.Duration) *ExHIncrArgs {
	a.Set[PX] = true
	a.px = px
	return a
}

func (a *ExHIncrArgs) ExAt(exAt time.Time) *ExHIncrArgs {
	a.Set[EXAT] = true
	a.exAt = exAt
	return a
}

func (a *ExHIncrArgs) PxAt(pxAt time.Time) *ExHIncrArgs {
	a.Set[PXAT] = true
	a.pxAt = pxAt
	return a
}

func (a *ExHIncrArgs) Ver(ver int64) *ExHIncrArgs {
	a.Set[VER] = true
	a.ver = ver
	return a
}

func (a *ExHIncrArgs) Abs(ver int64) *ExHIncrArgs {
	a.Set[ABS] = true
	a.ver = ver
	return a
}

func (a *ExHIncrArgs) Min(min int64) *ExHIncrArgs {
	a.Set[MIN] = true
	a.min = min
	return a
}

func (a *ExHIncrArgs) Max(max int64) *ExHIncrArgs {
	a.Set[MAX] = true
	a.max = max
	return a
}

func (a *ExHIncrArgs) Gt(gt int64) *ExHIncrArgs {
	a.Set[GT] = true
	a.gt = gt
	return a
}

func (a *ExHIncrArgs) KeepTTL() *ExHIncrArgs {
	a.Set[KEEPTTL] = true
	return a
}

func (a ExHIncrArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[EX]; ok {
		args = append(args, EX, a.ex)
	}
	if _, ok := a.Set[PX]; ok {
		args = append(args, PX, a.px)
	}
	if _, ok := a.Set[EXAT]; ok {
		args = append(args, EXAT, a.exAt.Unix())
	}
	if _, ok := a.Set[PXAT]; ok {
		args = append(args, PXAT, a.pxAt.Unix())
	}
	if _, ok := a.Set[VER]; ok {
		args = append(args, VER, a.ver)
	}
	if _, ok := a.Set[ABS]; ok {
		args = append(args, ABS, a.abs)
	}
	if _, ok := a.Set[MIN]; ok {
		args = append(args, MIN, a.min)
	}
	if _, ok := a.Set[MAX]; ok {
		args = append(args, MAX, a.max)
	}
	if _, ok := a.Set[GT]; ok {
		args = append(args, GT, a.gt)
	}
	if _, ok := a.Set[KEEPTTL]; ok {
		args = append(args, KEEPTTL)
	}
	return args
}

func (tc tairCmdable) ExHSet(ctx context.Context, key, field, value string) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exhset"
	a[1] = key
	a[2] = field
	a[3] = value
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHGet(ctx context.Context, key, field string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "exhget"
	a[1] = key
	a[2] = field
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHSetArgs(ctx context.Context, key, field, value string, arg *ExHSetArgs) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exhset"
	a[1] = key
	a[2] = field
	a[3] = value
	a = append(a, arg.GetArgs()...)
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHSetNx(ctx context.Context, key, field, value string) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exhsetnx"
	a[1] = key
	a[2] = field
	a[3] = value
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHMSet(ctx context.Context, key string, fieldValue map[string]string) *redis.StatusCmd {
	a := make([]interface{}, 2)
	a[0] = "exhmset"
	a[1] = key
	for k, v := range fieldValue {
		a = append(a, k)
		a = append(a, v)
	}
	cmd := redis.NewStatusCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHMSetWithOpts(ctx context.Context, key string, arg ...*ExHMSetWithOptsArgs) *redis.StatusCmd {
	a := make([]interface{}, 2)
	a[0] = "exhmsetwithopts"
	a[1] = key
	for _, p := range arg {
		a = append(a, p.GetArgs()...)
	}
	cmd := redis.NewStatusCmd(ctx, a...)
	return cmd
}

func (tc tairCmdable) ExHPExpire(ctx context.Context, key, field string, milliseconds int) *redis.BoolCmd {
	a := make([]interface{}, 4)
	a[0] = "exhpexpire"
	a[1] = key
	a[2] = field
	a[3] = milliseconds
	cmd := redis.NewBoolCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHPExpireAt(ctx context.Context, key, field string, unixTime int) *redis.BoolCmd {
	a := make([]interface{}, 4)
	a[0] = "exhpexpireat"
	a[1] = key
	a[2] = field
	a[3] = unixTime
	cmd := redis.NewBoolCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHExpire(ctx context.Context, key, field string, milliseconds int) *redis.BoolCmd {
	a := make([]interface{}, 4)
	a[0] = "exhexpire"
	a[1] = key
	a[2] = field
	a[3] = milliseconds
	cmd := redis.NewBoolCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHExpireAt(ctx context.Context, key, field string, unixTime int) *redis.BoolCmd {
	a := make([]interface{}, 4)
	a[0] = "exhexpireat"
	a[1] = key
	a[2] = field
	a[3] = unixTime
	cmd := redis.NewBoolCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHPTTL(ctx context.Context, key, field string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "exhpttl"
	a[1] = key
	a[2] = field
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHTTL(ctx context.Context, key, field string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "exhttl"
	a[1] = key
	a[2] = field
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHVer(ctx context.Context, key, field string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "exhver"
	a[1] = key
	a[2] = field
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHSetVer(ctx context.Context, key, field string, version int) *redis.BoolCmd {
	a := make([]interface{}, 4)
	a[0] = "exhsetver"
	a[1] = key
	a[2] = field
	a[3] = version
	cmd := redis.NewBoolCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHIncrBy(ctx context.Context, key, field string, value int) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exhincrby"
	a[1] = key
	a[2] = field
	a[3] = value
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHIncrByArgs(ctx context.Context, key, field string, value int, arg *ExHIncrArgs) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exhincrby"
	a[1] = key
	a[2] = field
	a[3] = value
	a = append(a, arg.GetArgs()...)
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHIncrByFloat(ctx context.Context, key, field string, value float64) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "exhincrbyfloat"
	a[1] = key
	a[2] = field
	a[3] = value
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHIncrByFloatArgs(ctx context.Context, key, field string, value float64, arg *ExHIncrArgs) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "exhincrbyfloat"
	a[1] = key
	a[2] = field
	a[3] = value
	a = append(a, arg.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHGetWithVer(ctx context.Context, key, field string) *redis.SliceCmd {
	a := make([]interface{}, 3)
	a[0] = "exhgetwithver"
	a[1] = key
	a[2] = field
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHMGet(ctx context.Context, key string, field ...string) *redis.StringSliceCmd {
	a := make([]interface{}, 2)
	a[0] = "exhmget"
	a[1] = key
	for _, f := range field {
		a = append(a, f)
	}
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHMGetWithVer(ctx context.Context, key string, field ...string) *redis.SliceCmd {
	a := make([]interface{}, 2)
	a[0] = "exhmgetwithver"
	a[1] = key
	for _, f := range field {
		a = append(a, f)
	}
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHDel(ctx context.Context, key string, field ...string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "exhdel"
	a[1] = key
	for _, f := range field {
		a = append(a, f)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHLen(ctx context.Context, key string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "exhlen"
	a[1] = key
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHExists(ctx context.Context, key, field string) *redis.BoolCmd {
	a := make([]interface{}, 3)
	a[0] = "exhexists"
	a[1] = key
	a[2] = field
	cmd := redis.NewBoolCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHStrLen(ctx context.Context, key, field string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "exhstrlen"
	a[1] = key
	a[2] = field
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHKeys(ctx context.Context, key string) *redis.StringSliceCmd {
	a := make([]interface{}, 2)
	a[0] = "exhkeys"
	a[1] = key
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHVals(ctx context.Context, key string) *redis.StringSliceCmd {
	a := make([]interface{}, 2)
	a[0] = "exhvals"
	a[1] = key
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHGetAll(ctx context.Context, key string) *redis.StringStringMapCmd {
	a := make([]interface{}, 2)
	a[0] = "exhgetall"
	a[1] = key
	cmd := redis.NewStringStringMapCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExHScan(ctx context.Context, key string, cursor string) *redis.SliceCmd {
	a := make([]interface{}, 3)
	a[0] = "exhscan"
	a[1] = key
	a[2] = cursor
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
