package tair

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type ExSetArgs struct {
	arg

	xx string
	nx string

	ex   time.Duration
	px   time.Duration
	exAt time.Time
	pxAt time.Time

	ver int64
	abs int64

	flags   int
	keepttl string
}

func (a ExSetArgs) New() *ExSetArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExSetArgs) Xx() *ExSetArgs {
	a.Set[XX] = true
	return a
}

func (a *ExSetArgs) Nx() *ExSetArgs {
	a.Set[NX] = true
	return a
}

func (a *ExSetArgs) Ex(ex time.Duration) *ExSetArgs {
	a.Set[EX] = true
	a.ex = ex
	return a
}

func (a *ExSetArgs) Px(px time.Duration) *ExSetArgs {
	a.Set[PX] = true
	a.px = px
	return a
}

func (a *ExSetArgs) ExAt(exAt time.Time) *ExSetArgs {
	a.Set[EXAT] = true
	a.exAt = exAt
	return a
}

func (a *ExSetArgs) PxAt(pxAt time.Time) *ExSetArgs {
	a.Set[PXAT] = true
	a.pxAt = pxAt
	return a
}

func (a *ExSetArgs) Ver(ver int64) *ExSetArgs {
	a.Set[VER] = true
	a.ver = ver
	return a
}

func (a *ExSetArgs) Abs(abs int64) *ExSetArgs {
	a.Set[ABS] = true
	a.abs = abs
	return a

}

func (a *ExSetArgs) Flags(flags int) *ExSetArgs {
	a.Set[FLAGS] = true
	a.flags = flags
	return a
}

func (a *ExSetArgs) KeepTTL() *ExSetArgs {
	a.Set[KEEPTTL] = true
	return a
}

func (a *ExSetArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[XX]; ok {
		args = append(args, XX)
	}
	if _, ok := a.Set[NX]; ok {
		args = append(args, NX)
	}
	if _, ok := a.Set[EX]; ok {
		args = append(args, EX, toSec(a.ex))
	}
	if _, ok := a.Set[PX]; ok {
		args = append(args, PX, toMs(a.px))
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
	if _, ok := a.Set[FLAGS]; ok {
		args = append(args, FLAGS, a.flags)
	}
	if _, ok := a.Set[KEEPTTL]; ok {
		args = append(args, KEEPTTL)
	}
	return args
}

type CasArgs struct {
	arg

	ex      time.Duration
	exAt    time.Time
	px      time.Duration
	pxAt    time.Time
	keepttl string
}

func (a CasArgs) New() *CasArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *CasArgs) Ex(ex time.Duration) *CasArgs {
	a.Set[EX] = true
	a.ex = ex
	return a
}

func (a *CasArgs) ExAt(exAt time.Time) *CasArgs {
	a.Set[EXAT] = true
	a.exAt = exAt
	return a
}

func (a *CasArgs) Px(px time.Duration) *CasArgs {
	a.Set[PX] = true
	a.px = px
	return a
}

func (a *CasArgs) PxAt(pxAt time.Time) *CasArgs {
	a.Set[PXAT] = true
	a.pxAt = pxAt
	return a
}

func (a *CasArgs) KeppTTL() *CasArgs {
	a.Set[KEEPTTL] = true
	return a
}

func (a *CasArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)

	if _, ok := a.Set[EXAT]; ok {
		args = append(args, EXAT, a.exAt.Unix())
	}
	if _, ok := a.Set[PXAT]; ok {
		args = append(args, PXAT, a.pxAt.Unix())
	}
	if _, ok := a.Set[EX]; ok {
		args = append(args, EX, toSec(a.ex))
	}
	if _, ok := a.Set[PX]; ok {
		args = append(args, PX, toMs(a.px))
	}
	if _, ok := a.Set[KEEPTTL]; ok {
		args = append(args, KEEPTTL)
	}
	return args
}

type ExIncrByArgs struct {
	arg

	xx string
	nx string

	ex   time.Duration
	px   time.Duration
	exAt time.Time
	pxAt time.Time

	ver int64
	abs int64

	min int64
	max int64

	def        int64
	noNegative string
	keepttl    string
}

func (a ExIncrByArgs) New() *ExIncrByArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExIncrByArgs) Xx() *ExIncrByArgs {
	a.Set[XX] = true
	return a
}

func (a *ExIncrByArgs) Nx() *ExIncrByArgs {
	a.Set[NX] = true
	return a
}

func (a *ExIncrByArgs) Ex(ex time.Duration) *ExIncrByArgs {
	a.Set[EX] = true
	a.ex = ex
	return a
}

func (a *ExIncrByArgs) Px(px time.Duration) *ExIncrByArgs {
	a.Set[PX] = true
	a.px = px
	return a
}

func (a *ExIncrByArgs) ExAt(exAt time.Time) *ExIncrByArgs {
	a.Set[EXAT] = true
	a.exAt = exAt
	return a
}

func (a *ExIncrByArgs) PxAt(pxAt time.Time) *ExIncrByArgs {
	a.Set[PXAT] = true
	a.pxAt = pxAt
	return a
}

func (a *ExIncrByArgs) Ver(ver int64) *ExIncrByArgs {
	a.Set[VER] = true
	a.ver = ver
	return a
}

func (a *ExIncrByArgs) Abs(ver int64) *ExIncrByArgs {
	a.Set[ABS] = true
	a.ver = ver
	return a
}

func (a *ExIncrByArgs) Min(min int64) *ExIncrByArgs {
	a.Set[MIN] = true
	a.min = min
	return a
}

func (a *ExIncrByArgs) Max(max int64) *ExIncrByArgs {
	a.Set[MAX] = true
	a.max = max
	return a
}

func (a *ExIncrByArgs) Def(def int64) *ExIncrByArgs {
	a.Set[DEF] = true
	a.def = def
	return a
}

func (a *ExIncrByArgs) SetNoNegative() *ExIncrByArgs {
	a.Set[NONEGATIVE] = true
	return a
}

func (a *ExIncrByArgs) KeepTTL() *ExIncrByArgs {
	a.Set[KEEPTTL] = true
	return a
}

func (a ExIncrByArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[XX]; ok {
		args = append(args, XX)
	}
	if _, ok := a.Set[NX]; ok {
		args = append(args, NX)
	}
	if _, ok := a.Set[EX]; ok {
		args = append(args, EX, toSec(a.ex))
	}
	if _, ok := a.Set[PX]; ok {
		args = append(args, PX, toMs(a.px))
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
	if _, ok := a.Set[DEF]; ok {
		args = append(args, DEF, a.def)
	}
	if _, ok := a.Set[NONEGATIVE]; ok {
		args = append(args, NONEGATIVE)
	}
	if _, ok := a.Set[KEEPTTL]; ok {
		args = append(args, KEEPTTL)
	}
	return args
}

func (tc tairCmdable) Cas(ctx context.Context, key string, oldVal, newVal interface{}) *redis.IntCmd {
	args := make([]interface{}, 4)
	args[0] = "cas"
	args[1] = key
	args[2] = oldVal
	args[3] = newVal
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CasArgs(ctx context.Context, key string, oldVal, newVal interface{}, a *CasArgs) *redis.IntCmd {
	args := make([]interface{}, 4)
	args[0] = "cas"
	args[1] = key
	args[2] = oldVal
	args[3] = newVal
	args = append(args, a.GetArgs()...)
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) Cad(ctx context.Context, key string, value interface{}) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "cad"
	args[1] = key
	args[2] = value
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExSet(ctx context.Context, key string, value interface{}) *redis.StatusCmd {
	args := make([]interface{}, 3)
	args[0] = "exset"
	args[1] = key
	args[2] = value
	cmd := redis.NewStatusCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExSetArgs(ctx context.Context, key string, value interface{}, a *ExSetArgs) *redis.StatusCmd {
	args := make([]interface{}, 3)
	args[0] = "exset"
	args[1] = key
	args[2] = value
	args = append(args, a.GetArgs()...)
	cmd := redis.NewStatusCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExSetWithVersion(ctx context.Context, key string, value interface{}, exSetParam ExSetArgs) *redis.IntCmd {
	args := make([]interface{}, 4)
	args[0] = "exset"
	args[1] = key
	args[2] = value
	args[3] = "withversion"
	args = append(args, exSetParam.GetArgs()...)
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExSetVer(ctx context.Context, key string, version int64) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "exset"
	args[1] = key
	args[2] = version
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExGet(ctx context.Context, key string) *redis.SliceCmd {
	cmd := redis.NewSliceCmd(ctx, "exget", key)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExGetWithFlags(ctx context.Context, key string) *redis.SliceCmd {
	cmd := redis.NewSliceCmd(ctx, "exget", key, "withflags")
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExIncrBy(ctx context.Context, key string, incr int64) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "exincrby", key, incr)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExIncrByArgs(ctx context.Context, key string, incr int64, a *ExIncrByArgs) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "exincrby"
	args[1] = key
	args[2] = incr
	args = append(args, a.GetArgs()...)
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExIncrByWithVersion(ctx context.Context, key string, incr int64, param ExIncrByArgs) *redis.SliceCmd {
	args := make([]interface{}, 3)
	args[0] = "exincrby"
	args[1] = key
	args[2] = incr
	args = append(args, param.GetArgs()...)
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExIncrByFloat(ctx context.Context, key string, incr float64) *redis.FloatCmd {
	args := make([]interface{}, 3)
	args[0] = "exincrbyfloat"
	args[1] = key
	args[2] = incr
	cmd := redis.NewFloatCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExIncrByFloatArgs(ctx context.Context, key string, incr float64, a *ExIncrByArgs) *redis.FloatCmd {
	args := make([]interface{}, 3)
	args[0] = "exincrbyfloat"
	args[1] = key
	args[2] = incr
	args = append(args, a.GetArgs()...)
	cmd := redis.NewFloatCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExCas(ctx context.Context, key string, newVal interface{}, version int64) *redis.SliceCmd {
	args := make([]interface{}, 4)
	args[0] = "excas"
	args[1] = key
	args[2] = newVal
	args[3] = version
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExCad(ctx context.Context, key string, version int) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "excad"
	args[1] = key
	args[2] = version
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExAppend(ctx context.Context, key string, value interface{}, nxxx, verAbs string, version int64) *redis.IntCmd {
	args := make([]interface{}, 6)
	args[0] = "exappend"
	args[1] = key
	args[2] = value
	args[3] = nxxx
	args[4] = verAbs
	args[5] = version
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExPreAppend(ctx context.Context, key string, value interface{}, nxxx, verAbs string, version int) *redis.IntCmd {
	args := make([]interface{}, 6)
	args[0] = "exprepend"
	args[1] = key
	args[2] = value
	args[3] = nxxx
	args[4] = verAbs
	args[5] = version
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExGae(ctx context.Context, key string, expxwithat string, time time.Duration) *redis.SliceCmd {
	args := make([]interface{}, 4)
	args[0] = "exgae"
	args[1] = key
	args[2] = expxwithat
	args[3] = time
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}
