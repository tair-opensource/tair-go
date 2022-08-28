package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
)

// args
type JsonSetArgs struct {
	arg
}

func (a JsonSetArgs) New() *JsonSetArgs {
	a.Set = make(map[string]bool, 0)
	return &a
}
func (a *JsonSetArgs) Xx() *JsonSetArgs {
	a.Set[XX] = true
	return a
}

func (a *JsonSetArgs) Nx() *JsonSetArgs {
	a.Set[NX] = true
	return a
}

func (a *JsonSetArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[XX]; ok {
		args = append(args, XX)
	}
	if _, ok := a.Set[NX]; ok {
		args = append(args, NX)
	}
	return args
}

type JsonGetArgs struct {
	arg
	format   string
	rootName string
	arrName  string
}

func (a *JsonGetArgs) Format(format string) *JsonGetArgs {
	a.Set[FORMAT] = true
	a.format = format
	return a
}

func (a *JsonGetArgs) RootName(rootName string) *JsonGetArgs {
	a.Set[ROOTNAME] = true
	a.rootName = rootName
	return a
}

func (a *JsonGetArgs) ArrName(arrName string) *JsonGetArgs {
	a.Set[ARRNAME] = true
	a.arrName = arrName
	return a
}

func (a JsonGetArgs) New() *JsonGetArgs {
	a.Set = make(map[string]bool, 0)
	return &a
}

func (a *JsonGetArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[FORMAT]; ok {
		args = append(args, FORMAT, a.format)
	}
	if _, ok := a.Set[ARRNAME]; ok {
		args = append(args, ARRNAME, a.arrName)
	}
	if _, ok := a.Set[ROOTNAME]; ok {
		args = append(args, ROOTNAME, a.rootName)
	}
	return args
}

func (tc tairCmdable) JsonSet(ctx context.Context, key string, path string, json string) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "JSON.SET"
	a[1] = key
	a[2] = path
	a[3] = json
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonSetArgs(ctx context.Context, key string, path string, json string, args *JsonSetArgs) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "JSON.SET"
	a[1] = key
	a[2] = path
	a[3] = json
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonGet(ctx context.Context, key string) *redis.StringCmd {
	a := make([]interface{}, 2)
	a[0] = "JSON.GET"
	a[1] = key
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonGetPath(ctx context.Context, key string, path string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.GET"
	a[1] = key
	a[2] = path
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonGetArgs(ctx context.Context, key string, path string, args *JsonGetArgs) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.GET"
	a[1] = key
	a[2] = path
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonDel(ctx context.Context, key string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "JSON.DEL"
	a[1] = key
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonDelPath(ctx context.Context, key string, path string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.DEL"
	a[1] = key
	a[2] = path
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonType(ctx context.Context, key string) *redis.StringCmd {
	a := make([]interface{}, 2)
	a[0] = "JSON.TYPE"
	a[1] = key
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonTypePath(ctx context.Context, key string, path string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.TYPE"
	a[1] = key
	a[2] = path
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonNumIncrBy(ctx context.Context, key string, value float64) *redis.FloatCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.NUMINCRBY"
	a[1] = key
	a[2] = value
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonNumIncrByWithPath(ctx context.Context, key string, path string, value float64) *redis.FloatCmd {
	a := make([]interface{}, 4)
	a[0] = "JSON.NUMINCRBY"
	a[1] = key
	a[2] = path
	a[3] = value
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) JsonStrAppend(ctx context.Context, key string, json string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.STRAPPEND"
	a[1] = key
	a[2] = json
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonStrAppendWithPath(ctx context.Context, key string, path string, json string) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "JSON.STRAPPEND"
	a[1] = key
	a[2] = path
	a[3] = json
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) JsonStrLen(ctx context.Context, key string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "JSON.STRLEN"
	a[1] = key
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonStrLenWithPath(ctx context.Context, key string, path string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.STRLEN"
	a[1] = key
	a[2] = path
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) JsonArrAppend(ctx context.Context, key string, jsons ...string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "JSON.ARRAPPEND"
	a[1] = key
	for _, json := range jsons {
		a = append(a, json)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonArrAppendWithPath(ctx context.Context, key string, path string, jsons ...string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.ARRAPPEND"
	a[1] = key
	a[2] = path
	for _, json := range jsons {
		a = append(a, json)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) JsonArrPop(ctx context.Context, key string, path string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.ARRPOP"
	a[1] = key
	a[2] = path
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonArrPopWithPath(ctx context.Context, key string, path string, index int64) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "JSON.ARRPOP"
	a[1] = key
	a[2] = path
	a[3] = index
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonArrInsert(ctx context.Context, args ...string) *redis.IntCmd {
	a := make([]interface{}, 1)
	a[0] = "JSON.ARRINSERT"
	for _, item := range args {
		a = append(a, item)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonArrLen(ctx context.Context, key string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "JSON.ARRLEN"
	a[1] = key
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonArrLenWithPath(ctx context.Context, key string, path string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "JSON.ARRLEN"
	a[1] = key
	a[2] = path
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) JsonArrTrim(ctx context.Context, key string, path string, start int64, stop int64) *redis.IntCmd {
	a := make([]interface{}, 5)
	a[0] = "JSON.ARRTRIM"
	a[1] = key
	a[2] = path
	a[3] = start
	a[4] = stop
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
