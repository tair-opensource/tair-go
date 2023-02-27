package tair

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type protocolType int

const (
	NONE protocolType = iota
	ProtoMatch
	ProtoCount
	ProtoMappings
	ProtoSettings
)

func (p protocolType) String() string {
	switch p {
	case NONE:
		return "NONE"
	case ProtoMatch:
		return "MATCH"
	case ProtoCount:
		return "COUNT"
	default:
		return "NA"
	}
}

type TftAddDocArgs struct {
	arg
}

func (a TftAddDocArgs) New() *TftAddDocArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *TftAddDocArgs) JoinArgs(key string, docs map[string]string) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, key)
	for k, v := range docs {
		args = append(args, k, v)
	}
	return args
}

type TftDelDocArgs struct {
	arg
}

func (a TftDelDocArgs) New() *TftDelDocArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *TftDelDocArgs) JoinArgs(key string, value ...string) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, key)
	for _, v := range value {
		args = append(args, v)
	}
	return args
}

type TftScanArgs struct {
	arg
	match string
	count int64
}

func (a TftScanArgs) New() *TftScanArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *TftScanArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[ProtoMatch.String()]; ok {
		args = append(args, ProtoMatch.String(), a.match)
	}
	if _, ok := a.Set[ProtoCount.String()]; ok {
		args = append(args, ProtoCount.String(), a.count)
	}
	return args
}

func (a *TftScanArgs) Match(pattern string) *TftScanArgs {
	a.Set[ProtoMatch.String()] = true
	a.match = pattern
	return a
}

// 这里为什么要用 Integer
func (a *TftScanArgs) Count(count int64) *TftScanArgs {
	a.Set[ProtoCount.String()] = true
	a.count = count
	return a
}

func (tc tairCmdable) TftMappingIndex(ctx context.Context, index, request string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.MAPPINGINDEX"
	a[1] = index
	a[2] = request
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftCreateIndex(ctx context.Context, index, request string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.CREATEINDEX"
	a[1] = index
	a[2] = request
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftUpdateIndex(ctx context.Context, index, request string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.UPDATEINDEX"
	a[1] = index
	a[2] = request
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftGetIndexMappings(ctx context.Context, index string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.GETINDEX"
	a[1] = index
	a[2] = "MAPPINGS"
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftGetIndex(ctx context.Context, index string) *redis.StringCmd {
	a := make([]interface{}, 2)
	a[0] = "TFT.GETINDEX"
	a[1] = index
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftGetIndexSettings(ctx context.Context, index string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.GETINDEX"
	a[1] = index
	a[2] = "SETTINGS"
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftAddDoc(ctx context.Context, index string, request string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.ADDDOC"
	a[1] = index
	a[2] = request
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftAddDocWithId(ctx context.Context, index string, request string, docId string) *redis.StringCmd {
	a := make([]interface{}, 5)
	a[0] = "TFT.ADDDOC"
	a[1] = index
	a[2] = request
	a[3] = "WITH_ID"
	a[4] = docId
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftMAddDoc(ctx context.Context, index string, docs map[string]string) *redis.StringCmd {
	a := make([]interface{}, 1)
	a[0] = "TFT.MADDDOC"
	a = append(a, TftAddDocArgs{}.New().JoinArgs(index, docs)...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftUpdateDocField(ctx context.Context, index, docId, docContent string) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "TFT.UPDATEDOCFIELD"
	a[1] = index
	a[2] = docId
	a[3] = docContent
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) TftIncrLongDocField(ctx context.Context, index, docId, docContent string, value int64) *redis.IntCmd {
	a := make([]interface{}, 5)
	a[0] = "TFT.INCRLONGDOCFIELD"
	a[1] = index
	a[2] = docId
	a[3] = docContent
	a[4] = value
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) TftIncrFloatDocField(ctx context.Context, index, docId, docContent string, value float64) *redis.FloatCmd {
	a := make([]interface{}, 5)
	a[0] = "TFT.INCRFLOATDOCFIELD"
	a[1] = index
	a[2] = docId
	a[3] = docContent
	a[4] = value
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftDelDocField(ctx context.Context, index, docId string, field ...string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.DELDOCFIELD"
	a[1] = index
	a[2] = docId
	for _, f := range field {
		a = append(a, f)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftGetDoc(ctx context.Context, index, docId string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.GETDOC"
	a[1] = index
	a[2] = docId
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftDelDoc(ctx context.Context, index string, docId ...string) *redis.StringCmd {
	a := make([]interface{}, 2)
	a[0] = "TFT.DELDOC"
	a[1] = index
	for _, d := range docId {
		a = append(a, d)
	}
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftDelAll(ctx context.Context, index string) *redis.StringCmd {
	a := make([]interface{}, 2)
	a[0] = "TFT.DELALL"
	a[1] = index
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftSearch(ctx context.Context, index string, request string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.SEARCH"
	a[1] = index
	a[2] = request
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftSearchUseCache(ctx context.Context, index string, request string, useCache bool) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.SEARCH"
	a[1] = index
	a[2] = request
	if useCache {
		a = append(a, "USE_CACHE")
	}
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftMSearch(ctx context.Context, indexCount int64, request string, index ...string) *redis.StringCmd {
	a := make([]interface{}, 2)
	a[0] = "TFT.MSEARCH"
	a[1] = indexCount
	for _, d := range index {
		a = append(a, d)
	}
	a = append(a, request)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftExists(ctx context.Context, index string, docId string) *redis.IntCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.EXISTS"
	a[1] = index
	a[2] = docId
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftDocNum(ctx context.Context, index string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "TFT.DOCNUM"
	a[1] = index
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftScanDocId(ctx context.Context, index string, cursor string) *redis.SliceCmd {
	a := make([]interface{}, 3)
	a[0] = "TFT.SCANDOCID"
	a[1] = index
	a[2] = cursor
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftScanDocIdArgs(ctx context.Context, index string, cursor string, a *TftScanArgs) *redis.SliceCmd {
	args := make([]interface{}, 3)
	args[0] = "TFT.SCANDOCID"
	args[1] = index
	args[2] = cursor
	args = append(args, a.GetArgs()...)
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftAddSug(ctx context.Context, index string, textWeight map[string]int64) *redis.IntCmd {
	args := make([]interface{}, 2)
	args[0] = "TFT.ADDSUG"
	args[1] = index
	for k, v := range textWeight {
		args = append(args, k, v)
	}
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftDelSug(ctx context.Context, index string, text ...string) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "TFT.DELSUG"
	args[1] = index
	for _, t := range text {
		args = append(args, t)
	}
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftSugSum(ctx context.Context, index string) *redis.IntCmd {
	args := make([]interface{}, 2)
	args[0] = "TFT.SUGNUM"
	args[1] = index
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftGetSug(ctx context.Context, index string, prefix string, count int8, fuzzy bool) *redis.StringSliceCmd {
	args := make([]interface{}, 5)
	args[0] = "TFT.GETSUG"
	args[1] = index
	args[2] = prefix
	args[3] = "MAX_COUNT"
	args[4] = count
	if fuzzy {
		args = append(args, "FUZZY")
	}
	cmd := redis.NewStringSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TftGetAllSug(ctx context.Context, index string) *redis.StringSliceCmd {
	args := make([]interface{}, 2)
	args[0] = "TFT.GETALLSUGS"
	args[1] = index
	cmd := redis.NewStringSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}
