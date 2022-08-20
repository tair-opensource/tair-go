package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
)

// struct

type ExTsDataPoint struct {
	sKey  string
	ts    string
	value float64
}

func (a *ExTsDataPoint) SKey() string {
	return a.sKey
}

func (a *ExTsDataPoint) SetSKey(sKey string) {
	a.sKey = sKey
}

func (a *ExTsDataPoint) Ts() string {
	return a.ts
}

func (a *ExTsDataPoint) SetTs(ts string) {
	a.ts = ts
}

func (a *ExTsDataPoint) Value() float64 {
	return a.value
}

func (a *ExTsDataPoint) SetValue(value float64) {
	a.value = value
}

type ExTsFilter struct {
	filter string
}

func (a *ExTsFilter) Filter() string {
	return a.filter
}

func (a *ExTsFilter) SetFilter(filter string) {
	a.filter = filter
}

// cmd
type ExTsLabelCmd struct {
	*redis.SliceCmd
	name  string
	value string
}

type ExTsSKeyCmd struct {
	*redis.SliceCmd
	sKey       string
	labels     []ExTsLabelCmd
	dataPoints []ExTsDataPointCmd
	token      int64
}

func (a *ExTsSKeyCmd) SKey() string {
	return a.sKey
}

func (a *ExTsSKeyCmd) SetSKey(sKey string) {
	a.sKey = sKey
}

func (a *ExTsSKeyCmd) Labels() []ExTsLabelCmd {
	return a.labels
}

func (a *ExTsSKeyCmd) SetLabels(labels []ExTsLabelCmd) {
	a.labels = labels
}

func (a *ExTsSKeyCmd) DataPoints() []ExTsDataPointCmd {
	return a.dataPoints
}

func (a *ExTsSKeyCmd) SetDataPoints(dataPoints []ExTsDataPointCmd) {
	a.dataPoints = dataPoints
}

func (a *ExTsSKeyCmd) Token() int64 {
	return a.token
}

func (a *ExTsSKeyCmd) SetToken(token int64) {
	a.token = token
}

func NewExTsSKeyCmd(ctx context.Context, arg ...interface{}) *ExTsSKeyCmd {
	return &ExTsSKeyCmd{
		SliceCmd: redis.NewSliceCmd(ctx, arg...),
	}
}

type ExTsDataPointCmd struct {
	*redis.SliceCmd
	ts    int64
	value string
}

func NewExTsDataPointCmd(ctx context.Context, arg ...interface{}) *ExTsDataPointCmd {
	return &ExTsDataPointCmd{
		SliceCmd: redis.NewSliceCmd(ctx, arg...),
	}
}

func (a *ExTsDataPointCmd) Ts() int64 {
	return a.ts
}

func (a *ExTsDataPointCmd) SetTs(ts int64) {
	a.ts = ts
}

func (a *ExTsDataPointCmd) Value() string {
	return a.value
}

func (a *ExTsDataPointCmd) SetValue(value string) {
	a.value = value
}

type ExTsSKeySliceCmd struct {
	*redis.SliceCmd
	val []ExTsSKeyCmd
}

func NewExTsSKeySliceCmd(ctx context.Context, arg ...interface{}) *ExTsSKeySliceCmd {
	return &ExTsSKeySliceCmd{
		SliceCmd: redis.NewSliceCmd(ctx, arg...),
	}
}

// args

type ExTsMAddArgs struct {
	arg
}

func (a ExTsMAddArgs) New() *ExTsMAddArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExTsMAddArgs) JoinArgs(pKey string, points []ExTsDataPoint) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, pKey)
	for _, p := range points {
		args = append(args, p.SKey(), p.Ts(), p.Value())
	}
	return args
}

type ExTsAttributeArgs struct {
	arg
	dataEt    int64
	chunkSize int64
	labels    []string
}

func (a *ExTsAttributeArgs) UnCompressed() *ExTsAttributeArgs {
	a.Set[UNCOMPRESSED] = true
	return a
}

func (a *ExTsAttributeArgs) DataEt(dataEt int64) *ExTsAttributeArgs {
	a.Set[DATA_ET] = true
	a.dataEt = dataEt
	return a
}

func (a *ExTsAttributeArgs) ChunkSize(chunkSize int64) *ExTsAttributeArgs {
	a.Set[CHUNK_SIZE] = true
	a.chunkSize = chunkSize
	return a
}

func (a *ExTsAttributeArgs) Labels(labels []string) *ExTsAttributeArgs {
	a.Set[LABELS] = true
	a.labels = labels
	return a
}

func (a ExTsAttributeArgs) New() *ExTsAttributeArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExTsAttributeArgs) JoinArgs(pKey string, points []ExTsDataPoint) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, pKey, len(points))
	for _, p := range points {
		args = append(args, p.SKey(), p.Ts(), p.Value())
	}
	return args
}
func (a *ExTsAttributeArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[UNCOMPRESSED]; ok {
		args = append(args, UNCOMPRESSED)
	}
	if _, ok := a.Set[DATA_ET]; ok {
		args = append(args, DATA_ET, a.dataEt)
	}
	if _, ok := a.Set[CHUNK_SIZE]; ok {
		args = append(args, CHUNK_SIZE, a.chunkSize)
	}
	if _, ok := a.Set[LABELS]; ok {
		args = append(args, LABELS, a.labels)
	}
	return args
}

// ExTsQueryArgs

type ExTsQueryArgs struct {
	arg
}

func (a ExTsQueryArgs) New() *ExTsQueryArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExTsQueryArgs) JoinArgs(pKey string, filters []*ExTsFilter) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, pKey)
	for _, f := range filters {
		args = append(args, f.Filter())
	}
	return args
}

type ExTsAggregationArgs struct {
	arg
	maxCount    int64
	withLabels  string
	reverse     string
	filter      string
	aggregation string
	min         int64
	max         int64
	sum         int64
	avg         int64
	stdp        int64
	stds        int64
	count       int64
	first       int64
	last        int64
	aggRange    int64
}

var MENU = [...]string{MIN, MAX, SUM, AVG, STDP, STDS, COUNT, FIRST, LAST, RANGE}

func (a ExTsAggregationArgs) New() *ExTsAggregationArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExTsAggregationArgs) WithLabels() *ExTsAggregationArgs {
	a.Set[WITHLABELS] = true
	return a
}

func (a *ExTsAggregationArgs) Reverse() *ExTsAggregationArgs {
	a.Set[REVERSE] = true
	return a
}

func (a *ExTsAggregationArgs) MaxCount(maxCount int64) *ExTsAggregationArgs {
	a.Set[MAXCOUNT] = true
	a.maxCount = maxCount
	return a
}

func (a *ExTsAggregationArgs) Min(timeBucket int64) *ExTsAggregationArgs {
	a.Set[MIN] = true
	a.min = timeBucket
	return a
}

func (a *ExTsAggregationArgs) Max(timeBucket int64) *ExTsAggregationArgs {
	a.Set[MAX] = true
	a.max = timeBucket
	return a
}
func (a *ExTsAggregationArgs) Sum(timeBucket int64) *ExTsAggregationArgs {
	a.Set[SUM] = true
	a.sum = timeBucket
	return a
}
func (a *ExTsAggregationArgs) Avg(timeBucket int64) *ExTsAggregationArgs {
	a.Set[AVG] = true
	a.avg = timeBucket
	return a
}
func (a *ExTsAggregationArgs) StdP(timeBucket int64) *ExTsAggregationArgs {
	a.Set[STDP] = true
	a.stdp = timeBucket
	return a
}

func (a *ExTsAggregationArgs) StdS(timeBucket int64) *ExTsAggregationArgs {
	a.Set[STDS] = true
	a.stds = timeBucket
	return a
}
func (a *ExTsAggregationArgs) Count(timeBucket int64) *ExTsAggregationArgs {
	a.Set[COUNT] = true
	a.count = timeBucket
	return a
}
func (a *ExTsAggregationArgs) First(timeBucket int64) *ExTsAggregationArgs {
	a.Set[FIRST] = true
	a.first = timeBucket
	return a
}

func (a *ExTsAggregationArgs) Last(timeBucket int64) *ExTsAggregationArgs {
	a.Set[LAST] = true
	a.last = timeBucket
	return a
}
func (a *ExTsAggregationArgs) Range(timeBucket int64) *ExTsAggregationArgs {
	a.Set[RANGE] = true
	a.aggRange = timeBucket
	return a
}

func (a *ExTsAggregationArgs) GetRangeArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[MAXCOUNT]; ok {
		args = append(args, MAXCOUNT)
	}
	if _, ok := a.Set[REVERSE]; ok {
		args = append(args, REVERSE)
	}
	// todo
	for _, menu := range MENU {
		if _, ok := a.Set[menu]; ok {
			args = append(args, AGGREGATION, menu)
			break
		}
	}
	return args
}

func (a *ExTsAggregationArgs) GetSRangeArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[MAXCOUNT]; ok {
		args = append(args, MAXCOUNT)
	}
	if _, ok := a.Set[WITHLABELS]; ok {
		args = append(args, WITHLABELS)
	}
	if _, ok := a.Set[REVERSE]; ok {
		args = append(args, REVERSE)
	}
	// todo
	for _, menu := range MENU {
		if _, ok := a.Set[menu]; ok {
			args = append(args, AGGREGATION, menu)
			break
		}
	}
	return args
}

func (a *ExTsAggregationArgs) GetMRangeArgs(filters []*ExTsFilter) []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[MAXCOUNT]; ok {
		args = append(args, MAXCOUNT)
	}
	// todo
	for _, menu := range MENU {
		if _, ok := a.Set[menu]; ok {
			args = append(args, AGGREGATION, menu)
			break
		}
	}
	if _, ok := a.Set[WITHLABELS]; ok {
		args = append(args, WITHLABELS)
	}
	if _, ok := a.Set[REVERSE]; ok {
		args = append(args, REVERSE)
	}
	args = append(args, FILTER)
	for _, filter := range filters {
		args = append(args, filter.Filter())
	}
	return args
}

func (a *ExTsAggregationArgs) GetPRangeArgs(filters []*ExTsFilter) []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[MAXCOUNT]; ok {
		args = append(args, MAXCOUNT)
	}
	// todo
	for _, menu := range MENU {
		if _, ok := a.Set[menu]; ok {
			args = append(args, AGGREGATION, menu)
			break
		}
	}
	if _, ok := a.Set[WITHLABELS]; ok {
		args = append(args, WITHLABELS)
	}
	if _, ok := a.Set[REVERSE]; ok {
		args = append(args, REVERSE)
	}
	args = append(args, FILTER)
	for _, filter := range filters {
		args = append(args, filter.Filter())
	}
	return args
}

// method

func (tc tairCmdable) TsPCreate(ctx context.Context, key string, value interface{}) *redis.StringCmd {
	args := make([]interface{}, 3)
	args[0] = "EXTS.P.CREATE"
	args[1] = key
	args[2] = value
	cmd := redis.NewStringCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TsSCreate(ctx context.Context, key string, value interface{}) *redis.StringCmd {
	args := make([]interface{}, 3)
	args[0] = "EXTS.S.CREATE"
	args[1] = key
	args[2] = value
	cmd := redis.NewStringCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsAdd(ctx context.Context, pKey, sKey, ts string, value float64) *redis.StringCmd {
	args := make([]interface{}, 5)
	args[0] = "EXTS.S.ADD"
	args[1] = pKey
	args[2] = sKey
	args[3] = ts
	args[4] = value
	cmd := redis.NewStringCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsAddArgs(ctx context.Context, pKey, sKey, ts string, value float64, args *ExTsAttributeArgs) *redis.StringCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.ADD"
	a[1] = pKey
	a[2] = sKey
	a[3] = ts
	a[4] = value
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMAdd(ctx context.Context, pKey string, sKeys []ExTsDataPoint) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.MADD"
	args := ExTsAttributeArgs{}.New()
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMAddArgs(ctx context.Context, pKey string, sKeys []ExTsDataPoint, args *ExTsAttributeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.MADD"
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsAlter(ctx context.Context, pKey, sKey string, args *ExTsAttributeArgs) *redis.StringCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.ALTER"
	a[1] = pKey
	a[2] = sKey
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsIncr(ctx context.Context, pKey, sKey, ts string, value float64) *redis.StringCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.INCRBY"
	a[1] = pKey
	a[2] = sKey
	a[3] = ts
	a[4] = value
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsIncrArgs(ctx context.Context, pKey, sKey, ts string, value float64, args *ExTsAttributeArgs) *redis.StringCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.INCRBY"
	a[1] = pKey
	a[2] = sKey
	a[3] = ts
	a[4] = value
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMIncr(ctx context.Context, pKey string, sKeys []ExTsDataPoint) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.MINCRBY"
	args := ExTsAttributeArgs{}.New()
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMIncrArgs(ctx context.Context, pKey string, sKeys []ExTsDataPoint, args *ExTsAttributeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.MINCRBY"
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsDel(ctx context.Context, pKey string, sKeys string) *redis.StringSliceCmd {
	a := make([]interface{}, 3)
	a[0] = "EXTS.S.DEL"
	a[1] = pKey
	a[2] = sKeys
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsGet(ctx context.Context, pKey string, sKeys string) *ExTsDataPointCmd {
	a := make([]interface{}, 3)
	a[0] = "EXTS.S.DEL"
	a[1] = pKey
	a[2] = sKeys
	cmd := NewExTsDataPointCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsQuery(ctx context.Context, pKey string, filters []*ExTsFilter) *redis.StringSliceCmd {
	a := make([]interface{}, 3)
	a[0] = "EXTS.S.QUERYINDEX"
	a[1] = pKey
	args := ExTsQueryArgs{}.New()
	a = append(a, args.JoinArgs(pKey, filters)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRange(ctx context.Context, pKey string, sKey string, startTs string, endTs string) *ExTsSKeyCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.RANGE"
	a[1] = pKey
	a[2] = sKey
	a[3] = startTs
	a[4] = endTs
	cmd := NewExTsSKeyCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRangeArgs(ctx context.Context, pKey string, sKey string, startTs string, endTs string, args *ExTsAggregationArgs) *ExTsSKeyCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.RANGE"
	a[1] = pKey
	a[2] = sKey
	a[3] = startTs
	a[4] = endTs
	a = append(a, args.GetArgs()...)
	cmd := NewExTsSKeyCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRange(ctx context.Context, pKey string, sKeys []string, startTs string, endTs string) *ExTsSKeySliceCmd {
	a := make([]interface{}, 2)
	a[0] = "EXTS.S.RANGE.KEYS"
	a[1] = pKey
	for _, sKey := range sKeys {
		a = append(a, sKey)
	}
	args := ExTsAggregationArgs{}.New()
	a = append(a, startTs, endTs)
	a = append(a, args.GetSRangeArgs()...)
	cmd := NewExTsSKeySliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRangeArgs(ctx context.Context, pKey string, sKeys []string, startTs string, endTs string, args *ExTsAggregationArgs) *ExTsSKeySliceCmd {
	a := make([]interface{}, 2)
	a[0] = "EXTS.S.RANGE.KEYS"
	a[1] = pKey
	for _, sKey := range sKeys {
		a = append(a, sKey)
	}
	a = append(a, startTs, endTs)
	a = append(a, args.GetSRangeArgs()...)
	cmd := NewExTsSKeySliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRangeFilter(ctx context.Context, pKey string, startTs string, endTs string, filters []*ExTsFilter) *ExTsSKeySliceCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.MRANGE"
	a[1] = pKey
	a[3] = startTs
	a[4] = endTs
	args := ExTsAggregationArgs{}.New()
	a = append(a, args.GetMRangeArgs(filters)...)
	cmd := NewExTsSKeySliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRangeFilterArgs(ctx context.Context, pKey string, startTs string, endTs string, filters []*ExTsFilter, args *ExTsAggregationArgs) *ExTsSKeySliceCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.MRANGE"
	a[1] = pKey
	a[3] = startTs
	a[4] = endTs
	a = append(a, args.GetMRangeArgs(filters)...)
	cmd := NewExTsSKeySliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsPRange(ctx context.Context, pKey string, startTs string, endTs string, pkeyAggregationType string, pkeyTimeBucket int64, filters []*ExTsFilter) *ExTsSKeySliceCmd {
	a := make([]interface{}, 7)
	a[0] = "EXTS.P.RANGE"
	a[1] = pKey
	a[3] = startTs
	a[4] = endTs
	a[5] = pkeyAggregationType
	a[6] = pkeyTimeBucket
	args := ExTsAggregationArgs{}.New()
	a = append(a, args.GetPRangeArgs(filters)...)
	cmd := NewExTsSKeySliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsPRangeArgs(ctx context.Context, pKey string, startTs string, endTs string, pkeyAggregationType string, pkeyTimeBucket int64, filters []*ExTsFilter, args *ExTsAggregationArgs) *ExTsSKeySliceCmd {
	a := make([]interface{}, 7)
	a[0] = "EXTS.P.RANGE"
	a[1] = pKey
	a[3] = startTs
	a[4] = endTs
	a[5] = pkeyAggregationType
	a[6] = pkeyTimeBucket
	a = append(a, args.GetPRangeArgs(filters)...)
	cmd := NewExTsSKeySliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRawModify(ctx context.Context, pKey string, ts string, value float64) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "EXTS.S.RAW_MODIFY"
	a[1] = pKey
	a[2] = ts
	a[3] = value
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRawModifyArgs(ctx context.Context, pKey string, ts string, value float64, args *ExTsAttributeArgs) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "EXTS.S.RAW_MODIFY"
	a[1] = pKey
	a[2] = ts
	a[3] = value
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRawModify(ctx context.Context, pKey string, sKeys []ExTsDataPoint) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.RAW_MODIFY"
	args := ExTsMAddArgs{}.New()
	a = append(a, args.JoinArgs(pKey, sKeys))
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRawModifyArgs(ctx context.Context, pKey string, sKeys []ExTsDataPoint, args *ExTsAttributeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.RAW_MODIFY"
	a = append(a, args.JoinArgs(pKey, sKeys))
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRawIncr(ctx context.Context, pKey string, sKey string, ts string, value float64) *redis.StringSliceCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.RAW_MODIFY"
	a[1] = pKey
	a[2] = sKey
	a[3] = ts
	a[4] = value
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRawIncrArgs(ctx context.Context, pKey string, sKey string, ts string, value float64, args *ExTsAttributeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.RAW_MODIFY"
	a[1] = pKey
	a[2] = sKey
	a[3] = ts
	a[4] = value
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRawIncr(ctx context.Context, pKey string, sKeys []ExTsDataPoint) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.RAW_MINCRBY"
	args := ExTsMAddArgs{}.New()
	a = append(a, args.JoinArgs(pKey, sKeys))
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRawIncrArgs(ctx context.Context, pKey string, sKeys []ExTsDataPoint, args *ExTsAttributeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.RAW_MINCRBY"
	a = append(a, args.JoinArgs(pKey, sKeys))
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
