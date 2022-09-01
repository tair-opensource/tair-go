package tair

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type BfInsertArgs struct {
	arg
	capacity  int64
	errorRate float64
}

func (a *BfInsertArgs) JoinArgs(key string, items ...string) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, key)
	if _, ok := a.Set[NOCREATE]; ok {
		args = append(args, NOCREATE)
	}
	args = append(args, CAPACITY, a.capacity, ERROR, a.errorRate)
	args = append(args, ITEMS)
	for _, item := range items {
		args = append(args, item)
	}
	return args
}

func (a BfInsertArgs) New() *BfInsertArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *BfInsertArgs) Capacity(initCapacity int64) *BfInsertArgs {
	a.Set[CAPACITY] = true
	a.capacity = initCapacity
	return a
}

func (a *BfInsertArgs) ErrorRate(errorRate float64) *BfInsertArgs {
	a.Set[ERROR] = true
	a.errorRate = errorRate
	return a
}

func (a *BfInsertArgs) NoCreate() *BfInsertArgs {
	a.Set[NOCREATE] = true
	return a
}

type BfMExistArgs struct {
	arg
}

func (a *BfMExistArgs) JoinArgs(key string, items ...string) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, key)
	for _, item := range items {
		args = append(args, item)
	}
	return args
}

func (a BfMExistArgs) New() *BfMExistArgs {
	a.Set = make(map[string]bool)
	return &a
}

type BfMAddArgs struct {
	arg
}

func (a *BfMAddArgs) JoinArgs(key string, items ...string) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, key)
	for _, item := range items {
		args = append(args, item)
	}
	return args
}

func (a BfMAddArgs) New() *BfMAddArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (tc tairCmdable) BfReserve(ctx context.Context, key string, initCapacity int64, errorRate float64) *redis.StringCmd {
	args := make([]interface{}, 4)
	args[0] = "BF.RESERVE"
	args[1] = key
	args[2] = errorRate
	args[3] = initCapacity
	cmd := redis.NewStringCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) BfAdd(ctx context.Context, key string, item string) *redis.BoolCmd {
	args := make([]interface{}, 3)
	args[0] = "BF.ADD"
	args[1] = key
	args[2] = item
	cmd := redis.NewBoolCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) BfMAdd(ctx context.Context, key string, items ...string) *redis.BoolSliceCmd {
	args := make([]interface{}, 1)
	args[0] = "BF.MADD"
	a := BfMAddArgs{}.New().JoinArgs(key, items...)
	args = append(args, a...)
	cmd := redis.NewBoolSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) BfExists(ctx context.Context, key string, item string) *redis.BoolCmd {
	args := make([]interface{}, 3)
	args[0] = "BF.EXISTS"
	args[1] = key
	args[2] = item
	cmd := redis.NewBoolCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) BfMExists(ctx context.Context, key string, items ...string) *redis.BoolSliceCmd {
	args := make([]interface{}, 1)
	args[0] = "BF.MEXISTS"
	a := BfMExistArgs{}.New().JoinArgs(key, items...)
	args = append(args, a...)
	cmd := redis.NewBoolSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) BfInsert(ctx context.Context, key string, bfInsertArgs *BfInsertArgs, items ...string) *redis.BoolSliceCmd {
	args := make([]interface{}, 1)
	args[0] = "BF.INSERT"
	args = append(args, bfInsertArgs.JoinArgs(key, items...)...)
	cmd := redis.NewBoolSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) BfDebug(ctx context.Context, key string) *redis.StringSliceCmd {
	args := make([]interface{}, 2)
	args[0] = "BF.DEBUG"
	args[1] = key
	cmd := redis.NewStringSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}
