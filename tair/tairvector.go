package tair

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type TvsCreateIndexArgs struct {
	arg
	dataType         string
	m                int32
	efConstruct      int32
	autoGc           bool
	lexicalAlgorithm string
	analyzer         string
	k1               float32
	b                float32
	hybridRatio      float32
}

func (a TvsCreateIndexArgs) New() *TvsCreateIndexArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *TvsCreateIndexArgs) DataType(dataType string) *TvsCreateIndexArgs {
	a.Set["data_type"] = true
	a.dataType = dataType
	return a
}

func (a *TvsCreateIndexArgs) M(m int32) *TvsCreateIndexArgs {
	a.Set["M"] = true
	a.m = m
	return a
}

func (a *TvsCreateIndexArgs) EfConstruct(efConstruct int32) *TvsCreateIndexArgs {
	a.Set["ef_construct"] = true
	a.efConstruct = efConstruct
	return a
}

func (a *TvsCreateIndexArgs) AutoGc(autoGc bool) *TvsCreateIndexArgs {
	a.Set["auto_gc"] = true
	a.autoGc = autoGc
	return a
}

func (a *TvsCreateIndexArgs) LexicalAlgorithm(lexicalAlgorithm string) *TvsCreateIndexArgs {
	a.Set["lexical_algorithm"] = true
	a.lexicalAlgorithm = lexicalAlgorithm
	return a
}

func (a *TvsCreateIndexArgs) Analyzer(analyzer string) *TvsCreateIndexArgs {
	a.Set["analyzer"] = true
	a.analyzer = analyzer
	return a
}

func (a *TvsCreateIndexArgs) K1(k1 float32) *TvsCreateIndexArgs {
	a.Set["k1"] = true
	a.k1 = k1
	return a
}

func (a *TvsCreateIndexArgs) B(b float32) *TvsCreateIndexArgs {
	a.Set["b"] = true
	a.b = b
	return a
}

func (a *TvsCreateIndexArgs) HybridRatio(hybridRatio float32) *TvsCreateIndexArgs {
	a.Set["hybrid_ratio"] = true
	a.hybridRatio = hybridRatio
	return a
}

func (a *TvsCreateIndexArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set["data_type"]; ok {
		args = append(args, "data_type", a.dataType)
	}
	if _, ok := a.Set["M"]; ok {
		args = append(args, "M", a.m)
	}
	if _, ok := a.Set["ef_construct"]; ok {
		args = append(args, "ef_construct", a.efConstruct)
	}
	if _, ok := a.Set["auto_gc"]; ok {
		args = append(args, "auto_gc", a.autoGc)
	}
	if _, ok := a.Set["lexical_algorithm"]; ok {
		args = append(args, "lexical_algorithm", a.lexicalAlgorithm)
	}
	if _, ok := a.Set["analyzer"]; ok {
		args = append(args, "analyzer", a.analyzer)
	}
	if _, ok := a.Set["k1"]; ok {
		args = append(args, "k1", a.k1)
	}
	if _, ok := a.Set["b"]; ok {
		args = append(args, "b", a.b)
	}
	if _, ok := a.Set["hybrid_ratio"]; ok {
		args = append(args, "hybrid_ratio", a.hybridRatio)
	}
	return args
}

func (tc tairCmdable) TvsCreateIndex(ctx context.Context, name string, dim int, indexType string, distanceType string, a *TvsCreateIndexArgs) *redis.StatusCmd {
	args := make([]interface{}, 5)
	args[0] = "TVS.CREATEINDEX"
	args[1] = name
	args[2] = dim
	args[3] = indexType
	args[4] = distanceType
	if a != nil {
		args = append(args, a.GetArgs()...)
	}
	cmd := redis.NewStatusCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsGetIndex(ctx context.Context, name string) *redis.SliceCmd {
	args := make([]interface{}, 2)
	args[0] = "TVS.GETINDEX"
	args[1] = name
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsDelIndex(ctx context.Context, name string) *redis.IntCmd {
	args := make([]interface{}, 2)
	args[0] = "TVS.DELINDEX"
	args[1] = name
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

type TvsScanIndexArgs struct {
	arg
	pattern string
	count   int
}

func (a TvsScanIndexArgs) New() *TvsScanIndexArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *TvsScanIndexArgs) Pattern(pattern string) *TvsScanIndexArgs {
	a.Set["Pattern"] = true
	a.pattern = pattern
	return a
}

func (a *TvsScanIndexArgs) Count(count int) *TvsScanIndexArgs {
	a.Set["COUNT"] = true
	a.count = count
	return a
}

func (a *TvsScanIndexArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set["Pattern"]; ok {
		args = append(args, "MATCH", a.pattern)
	}
	if _, ok := a.Set["COUNT"]; ok {
		args = append(args, "COUNT", a.count)
	}
	return args
}

func (tc tairCmdable) TvsScanIndex(ctx context.Context, cursor string, a *TvsScanIndexArgs) *redis.SliceCmd {
	args := make([]interface{}, 2)
	args[0] = "TVS.SCANINDEX"
	args[1] = cursor
	if a != nil {
		args = append(args, a.GetArgs()...)
	}
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

type TvsHSetArgs struct {
	arg
	fields map[string]interface{}
}

func (a TvsHSetArgs) New() *TvsHSetArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *TvsHSetArgs) Fields(fields map[string]interface{}) *TvsHSetArgs {
	a.Set["fields"] = true
	a.fields = fields
	return a
}

func (a *TvsHSetArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set["fields"]; ok {
		for filed, value := range a.fields {
			args = append(args, filed, value)
		}
	}
	return args
}

func (tc tairCmdable) TvsHSet(ctx context.Context, index string, key string, a *TvsHSetArgs) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "TVS.HSET"
	args[1] = index
	args[2] = key
	args = append(args, a.GetArgs()...)
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHGetAll(ctx context.Context, index string, key string) *redis.SliceCmd {
	args := make([]interface{}, 3)
	args[0] = "TVS.HGETALL"
	args[1] = index
	args[2] = key
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHMGet(ctx context.Context, index string, key string, fields []string) *redis.SliceCmd {
	args := make([]interface{}, 3)
	args[0] = "TVS.HMGET"
	args[1] = index
	args[2] = key
	if fields != nil {
		for _, field := range fields {
			args = append(args, field)
		}
	}
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsDel(ctx context.Context, index string, keys []string) *redis.IntCmd {
	args := make([]interface{}, 2)
	args[0] = "TVS.DEL"
	args[1] = index
	for _, key := range keys {
		args = append(args, key)
	}
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHDel(ctx context.Context, index string, key string, fields []string) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "TVS.HDEL"
	args[1] = index
	args[2] = key
	for _, field := range fields {
		args = append(args, field)
	}
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

type TvsScanArgs struct {
	arg
	pattern string
	count   int
	filter  string
	vector  string
	maxDist float32
}

func (a TvsScanArgs) New() *TvsScanArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *TvsScanArgs) Pattern(pattern string) *TvsScanArgs {
	a.Set["Pattern"] = true
	a.pattern = pattern
	return a
}

func (a *TvsScanArgs) Count(count int) *TvsScanArgs {
	a.Set["COUNT"] = true
	a.count = count
	return a
}

func (a *TvsScanArgs) Filter(filter string) *TvsScanArgs {
	a.Set["Filter"] = true
	a.filter = filter
	return a
}

func (a *TvsScanArgs) Vector(vector string) *TvsScanArgs {
	a.Set["Vector"] = true
	a.vector = vector
	return a
}

func (a *TvsScanArgs) MaxDist(maxDist float32) *TvsScanArgs {
	a.Set["MaxDist"] = true
	a.maxDist = maxDist
	return a
}

func (a *TvsScanArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set["Pattern"]; ok {
		args = append(args, "MATCH", a.pattern)
	}
	if _, ok := a.Set["COUNT"]; ok {
		args = append(args, "COUNT", a.count)
	}
	if _, ok := a.Set["Filter"]; ok {
		args = append(args, "FILTER", a.filter)
	}
	if _, ok := a.Set["Vector"]; ok {
		args = append(args, "VECTOR", a.vector)
	}
	if _, ok := a.Set["MaxDist"]; ok {
		args = append(args, "MAX_DIST", a.maxDist)
	}
	return args
}

func (tc tairCmdable) TvsScan(ctx context.Context, index string, cursor string, a *TvsScanArgs) *redis.SliceCmd {
	args := make([]interface{}, 3)
	args[0] = "TVS.SCAN"
	args[1] = index
	args[2] = cursor
	args = append(args, a.GetArgs()...)
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHIncrBy(ctx context.Context, index string, key string, field string, value int64) *redis.IntCmd {
	args := make([]interface{}, 5)
	args[0] = "TVS.HINCRBY"
	args[1] = index
	args[2] = key
	args[3] = field
	args[4] = value
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHIncrByFloat(ctx context.Context, index string, key string, field string, value float64) *redis.FloatCmd {
	args := make([]interface{}, 5)
	args[0] = "TVS.HINCRBYFLOAT"
	args[1] = index
	args[2] = key
	args[3] = field
	args[4] = value
	cmd := redis.NewFloatCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHPExpire(ctx context.Context, index string, key string, milliseconds int) *redis.IntCmd {
	args := make([]interface{}, 4)
	args[0] = "TVS.HPEXPIRE"
	args[1] = index
	args[2] = key
	args[3] = milliseconds
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHPExpireAt(ctx context.Context, index string, key string, milliUnixTime int) *redis.IntCmd {
	args := make([]interface{}, 4)
	args[0] = "TVS.HPEXPIREAT"
	args[1] = index
	args[2] = key
	args[3] = milliUnixTime
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHExpire(ctx context.Context, index string, key string, seconds int) *redis.IntCmd {
	args := make([]interface{}, 4)
	args[0] = "TVS.HEXPIRE"
	args[1] = index
	args[2] = key
	args[3] = seconds
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHExpireAt(ctx context.Context, index string, key string, unixTime int) *redis.IntCmd {
	args := make([]interface{}, 4)
	args[0] = "TVS.HEXPIREAT"
	args[1] = index
	args[2] = key
	args[3] = unixTime
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHPTTL(ctx context.Context, index string, key string) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "TVS.HPTTL"
	args[1] = index
	args[2] = key
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHTTL(ctx context.Context, index string, key string) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "TVS.HTTL"
	args[1] = index
	args[2] = key
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHPExpireTime(ctx context.Context, index string, key string) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "TVS.HPEXPIRETIME"
	args[1] = index
	args[2] = key
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsHExpireTime(ctx context.Context, index string, key string) *redis.IntCmd {
	args := make([]interface{}, 3)
	args[0] = "TVS.HEXPIRETIME"
	args[1] = index
	args[2] = key
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

type TvsKnnSearchArgs struct {
	arg
	filter      string
	efSearch    int
	maxDist     float32
	text        string
	hybridRatio float32
}

func (a TvsKnnSearchArgs) New() *TvsKnnSearchArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *TvsKnnSearchArgs) Filter(filter string) *TvsKnnSearchArgs {
	a.Set["Filter"] = true
	a.filter = filter
	return a
}

func (a *TvsKnnSearchArgs) EfSearch(efSearch int) *TvsKnnSearchArgs {
	a.Set["EfSearch"] = true
	a.efSearch = efSearch
	return a
}

func (a *TvsKnnSearchArgs) MaxDist(maxDist float32) *TvsKnnSearchArgs {
	a.Set["MaxDist"] = true
	a.maxDist = maxDist
	return a
}

func (a *TvsKnnSearchArgs) Text(text string) *TvsKnnSearchArgs {
	a.Set["Text"] = true
	a.text = text
	return a
}

func (a *TvsKnnSearchArgs) HybridRatio(hybridRatio float32) *TvsKnnSearchArgs {
	a.Set["HybridRatio"] = true
	a.hybridRatio = hybridRatio
	return a
}

func (a *TvsKnnSearchArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set["Filter"]; ok {
		args = append(args, a.filter)
	}
	if _, ok := a.Set["EfSearch"]; ok {
		args = append(args, "ef_search", a.efSearch)
	}
	if _, ok := a.Set["MaxDist"]; ok {
		args = append(args, "MAX_DIST", a.maxDist)
	}
	if _, ok := a.Set["Text"]; ok {
		args = append(args, "TEXT", a.text)
	}
	if _, ok := a.Set["HybridRatio"]; ok {
		args = append(args, "hybrid_ratio", a.hybridRatio)
	}
	return args
}

func (tc tairCmdable) TvsKnnSearch(ctx context.Context, index string, topN int, vector string, a *TvsKnnSearchArgs) *redis.SliceCmd {
	args := make([]interface{}, 4)
	args[0] = "TVS.KNNSEARCH"
	args[1] = index
	args[2] = topN
	args[3] = vector
	if a != nil {
		args = append(args, a.GetArgs()...)
	}
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

type TvsGetDistanceArgs struct {
	arg
	topN    int
	filter  string
	maxDist float32
}

func (a TvsGetDistanceArgs) New() *TvsGetDistanceArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *TvsGetDistanceArgs) TopN(topN int) *TvsGetDistanceArgs {
	a.Set["TopN"] = true
	a.topN = topN
	return a
}

func (a *TvsGetDistanceArgs) Filter(filter string) *TvsGetDistanceArgs {
	a.Set["Filter"] = true
	a.filter = filter
	return a
}

func (a *TvsGetDistanceArgs) MaxDist(maxDist float32) *TvsGetDistanceArgs {
	a.Set["MaxDist"] = true
	a.maxDist = maxDist
	return a
}

func (a *TvsGetDistanceArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set["TopN"]; ok {
		args = append(args, "TOPN", a.topN)
	}
	if _, ok := a.Set["Filter"]; ok {
		args = append(args, "FILTER", a.filter)
	}
	if _, ok := a.Set["MaxDist"]; ok {
		args = append(args, "MAX_DIST", a.maxDist)
	}
	return args
}

func (tc tairCmdable) TvsGetDistance(ctx context.Context, index string, vector string, keys []string, a *TvsGetDistanceArgs) *redis.SliceCmd {
	args := make([]interface{}, 4)
	args[0] = "TVS.GETDISTANCE"
	args[1] = index
	args[2] = vector
	args[3] = len(keys)
	for _, key := range keys {
		args = append(args, key)
	}
	if a != nil {
		args = append(args, a.GetArgs()...)
	}
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsMKnnSearch(ctx context.Context, index string, topN int, vectors []string, a *TvsKnnSearchArgs) *redis.SliceCmd {
	args := make([]interface{}, 4)
	args[0] = "TVS.MKNNSEARCH"
	args[1] = index
	args[2] = topN
	args[3] = len(vectors)
	for _, vector := range vectors {
		args = append(args, vector)
	}
	if a != nil {
		args = append(args, a.GetArgs()...)
	}
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsMIndexKnnSearch(ctx context.Context, indexes []string, topN int, vector string, a *TvsKnnSearchArgs) *redis.SliceCmd {
	args := make([]interface{}, 2)
	args[0] = "TVS.MINDEXKNNSEARCH"
	args[1] = len(indexes)
	for _, index := range indexes {
		args = append(args, index)
	}
	args = append(args, topN)
	args = append(args, vector)
	if a != nil {
		args = append(args, a.GetArgs()...)
	}
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TvsMIndexMKnnSearch(ctx context.Context, indexes []string, topN int, vectors []string, a *TvsKnnSearchArgs) *redis.SliceCmd {
	args := make([]interface{}, 2)
	args[0] = "TVS.MINDEXMKNNSEARCH"
	args[1] = len(indexes)
	for _, index := range indexes {
		args = append(args, index)
	}
	args = append(args, topN)
	args = append(args, len(vectors))
	for _, vector := range vectors {
		args = append(args, vector)
	}
	if a != nil {
		args = append(args, a.GetArgs()...)
	}
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}
