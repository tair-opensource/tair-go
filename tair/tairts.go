package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strconv"
)

var MENUS = [...]string{MIN, MAX, SUM, AVG, STDP, STDS, COUNT, FIRST, LAST, RANGE}

// struct

type ExTsDataPoint struct {
	sKey  string
	ts    string
	value float64
}

func (a *ExTsDataPoint) SKey() string {
	return a.sKey
}

func (a *ExTsDataPoint) SetSKey(sKey string) *ExTsDataPoint {
	a.sKey = sKey
	return a
}

func (a *ExTsDataPoint) Ts() string {
	return a.ts
}

func (a *ExTsDataPoint) SetTs(ts string) *ExTsDataPoint {
	a.ts = ts
	return a
}

func (a *ExTsDataPoint) Value() float64 {
	return a.value
}

func (a *ExTsDataPoint) SetValue(value float64) *ExTsDataPoint {
	a.value = value
	return a
}

type ExTsFilter struct {
	filter string
}

func (a *ExTsFilter) Filter() string {
	return a.filter
}

func (a *ExTsFilter) SetFilter(filter string) *ExTsFilter {
	a.filter = filter
	return a
}

// cmd

type ExTsLabelCmd struct {
	*redis.SliceCmd
	name  string
	value string
}

func (cmd *ExTsLabelCmd) Value() string {
	return cmd.value
}
func (cmd *ExTsLabelCmd) Name() string {
	return cmd.name
}

func (cmd *ExTsLabelCmd) BuildForTsMRangeString(exTsLabelCmd interface{}) *ExTsLabelCmd {
	c := exTsLabelCmd.([]interface{})
	cmd.name = c[0].(string)
	cmd.value = c[1].(string)
	return cmd
}

type ExTsSKeyCmd struct {
	*redis.SliceCmd
	sKey       string
	labels     []*ExTsLabelCmd
	dataPoints []*ExTsDataPointCmd
	token      int64
}

func (cmd *ExTsSKeyCmd) BuildExTsSKeyCmd() *ExTsSKeyCmd {
	val := cmd.Val()
	dataPoints := val[0].([]interface{})
	e := &ExTsSKeyCmd{}
	e.labels = make([]*ExTsLabelCmd, 0)
	dataPointsSlice := make([]*ExTsDataPointCmd, 0)
	for _, dataPoint := range dataPoints {
		point := dataPoint.([]interface{})
		p := &ExTsDataPointCmd{}
		p.ts = point[0].(int64)
		p.value = point[1].(string)
		dataPointsSlice = append(dataPointsSlice, p)
	}
	cmd.dataPoints = dataPointsSlice
	return cmd
}

func (cmd *ExTsSKeyCmd) BuildForExTsRangeString(exTsSKeyCmd interface{}) *ExTsSKeyCmd {
	c := exTsSKeyCmd.([]interface{})
	cmd.sKey = c[0].(string)
	cmd.token = c[3].(int64)
	// labels
	labelSlice := make([]*ExTsLabelCmd, 0)
	for _, cmdItem := range c[1].([]interface{}) {
		labelCmd := &ExTsLabelCmd{}
		labelCmd.BuildForTsMRangeString(cmdItem)
		labelSlice = append(labelSlice, labelCmd)
	}
	cmd.labels = labelSlice
	// dataPoints
	dataPointSlice := make([]*ExTsDataPointCmd, 0)
	for _, cmdItem := range c[2].([]interface{}) {
		dataPointCmd := &ExTsDataPointCmd{}
		dataPointCmd.BuildForTsMRangeString(cmdItem)
		dataPointSlice = append(dataPointSlice, dataPointCmd)
	}
	cmd.dataPoints = dataPointSlice
	return cmd
}

func (a *ExTsSKeyCmd) SKey() string {
	return a.sKey
}

func (a *ExTsSKeyCmd) SetSKey(sKey string) {
	a.sKey = sKey
}

func (a *ExTsSKeyCmd) Labels() []*ExTsLabelCmd {
	return a.labels
}

func (a *ExTsSKeyCmd) SetLabels(labels []*ExTsLabelCmd) {
	a.labels = labels
}

func (a *ExTsSKeyCmd) DataPoints() []*ExTsDataPointCmd {
	return a.dataPoints
}

func (a *ExTsSKeyCmd) SetDataPoints(dataPoints []*ExTsDataPointCmd) {
	a.dataPoints = dataPoints
}

func (a *ExTsSKeyCmd) Token() int64 {
	return a.token
}

func (a *ExTsSKeyCmd) SetToken(token int64) {
	a.token = token
}

func NewExTsSKeyCmd(cmd *redis.SliceCmd) *ExTsSKeyCmd {
	c := &ExTsSKeyCmd{
		SliceCmd: cmd,
	}
	c.BuildExTsSKeyCmd() // build self from built-in val
	return c
}

func (cmd *ExTsSKeyCmd) Result() (*ExTsSKeyCmd, error) {
	return cmd, cmd.Err()
}

func (c *ExTsSKeyCmd) BuildForExTsRange() *ExTsSKeyCmd {
	valSlice := c.Val()
	if len(valSlice) == 0 {
		return nil
	}
	dataPoint := valSlice[0].([]interface{})
	c.sKey = ""
	dataPointSlice := make([]*ExTsDataPointCmd, 0)
	for _, item := range dataPoint {
		tmpItem := item.([]interface{})
		point := &ExTsDataPointCmd{
			ts:    tmpItem[0].(int64),
			value: tmpItem[1].(string),
		}
		dataPointSlice = append(dataPointSlice, point)
	}
	c.dataPoints = dataPointSlice
	c.labels = make([]*ExTsLabelCmd, 0)
	c.token = valSlice[1].(int64)
	return c
}

type ExTsDataPointCmd struct {
	*redis.SliceCmd
	ts    int64
	value string
}

func (cmd *ExTsDataPointCmd) BuildForTsMRangeString(exTsDAtaPointCmd interface{}) *ExTsDataPointCmd {
	c := exTsDAtaPointCmd.([]interface{})
	cmd.ts = c[0].(int64)
	cmd.value = c[1].(string)
	return cmd
}

func (cmd *ExTsDataPointCmd) Result() (*ExTsDataPointCmd, error) {
	return cmd, cmd.Err()
}

func NewExTsDataPointCmd(sliceCmd *redis.SliceCmd) *ExTsDataPointCmd {
	cmd := &ExTsDataPointCmd{
		SliceCmd: sliceCmd,
	}
	cmd.BuildForTsMRangeString(cmd.Val())
	return cmd
}

func (cmd *ExTsDataPointCmd) Ts() int64 {
	return cmd.ts
}

func (cmd *ExTsDataPointCmd) SetTs(ts int64) {
	cmd.ts = ts
}

func (cmd *ExTsDataPointCmd) Value() float64 {
	val, err := strconv.ParseFloat(cmd.value, 64)
	if err != nil {
		panic("cannot parse float")
	}
	return val
}

func (cmd *ExTsDataPointCmd) SetValue(value string) {
	cmd.value = value
}

type ExTsSKeySliceCmd struct {
	*redis.SliceCmd
	val []*ExTsSKeyCmd
}

func NewExTsSKeySliceCmd(cmd *redis.SliceCmd) *ExTsSKeySliceCmd {
	c := &ExTsSKeySliceCmd{
		SliceCmd: cmd,
	}
	c.BuildForExTsMRangeString()
	return c
}

func (cmd *ExTsSKeySliceCmd) BuildForExTsMRangeString() *ExTsSKeySliceCmd {
	exTsSKeyCmdSlice := make([]*ExTsSKeyCmd, 0)
	for _, item := range cmd.Val() {
		exTsSKeyCmd := &ExTsSKeyCmd{}
		exTsSKeyCmd.BuildForExTsRangeString(item)
		exTsSKeyCmdSlice = append(exTsSKeyCmdSlice, exTsSKeyCmd)
	}
	cmd.val = exTsSKeyCmdSlice
	return cmd
}

func (cmd *ExTsSKeySliceCmd) Result() ([]*ExTsSKeyCmd, error) {
	return cmd.val, cmd.Err()
}

// args

type ExTsMAddArgs struct {
	arg
}

func (a ExTsMAddArgs) New() *ExTsMAddArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExTsMAddArgs) JoinArgs(pKey string, points []*ExTsDataPoint) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, pKey)
	args = append(args, len(points))
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

func (a *ExTsAttributeArgs) JoinArgs(pKey string, points []*ExTsDataPoint) []interface{} {
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
		args = append(args, LABELS)
		for _, label := range a.labels {
			args = append(args, label)
		}
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
	Map      map[string]interface{}
	maxCount int64
	min      int64
	max      int64
	sum      int64
	avg      int64
	stdp     int64
	stds     int64
	count    int64
	first    int64
	last     int64
	aggRange int64
}

func (a ExTsAggregationArgs) New() *ExTsAggregationArgs {
	a.Set = make(map[string]bool)
	a.Map = make(map[string]interface{})
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
	a.Map[MAXCOUNT] = maxCount
	return a
}

func (a *ExTsAggregationArgs) Min(min int64) *ExTsAggregationArgs {
	a.Set[MIN] = true
	a.min = min
	a.Map[MIN] = min
	return a
}

func (a *ExTsAggregationArgs) Max(max int64) *ExTsAggregationArgs {
	a.Set[MAX] = true
	a.max = max
	a.Map[MAX] = max
	return a
}
func (a *ExTsAggregationArgs) Sum(sum int64) *ExTsAggregationArgs {
	a.Set[SUM] = true
	a.sum = sum
	a.Map[SUM] = sum
	return a
}
func (a *ExTsAggregationArgs) Avg(avg int64) *ExTsAggregationArgs {
	a.Set[AVG] = true
	a.avg = avg
	a.Map[AVG] = avg
	return a
}
func (a *ExTsAggregationArgs) StdP(stdP int64) *ExTsAggregationArgs {
	a.Set[STDP] = true
	a.stdp = stdP
	a.Map[STDP] = stdP
	return a
}

func (a *ExTsAggregationArgs) StdS(stdS int64) *ExTsAggregationArgs {
	a.Set[STDS] = true
	a.stds = stdS
	a.Map[STDS] = stdS
	return a
}
func (a *ExTsAggregationArgs) Count(count int64) *ExTsAggregationArgs {
	a.Set[COUNT] = true
	a.count = count
	a.Map[COUNT] = count
	return a
}
func (a *ExTsAggregationArgs) First(first int64) *ExTsAggregationArgs {
	a.Set[FIRST] = true
	a.first = first
	a.Map[FIRST] = first

	return a
}

func (a *ExTsAggregationArgs) Last(last int64) *ExTsAggregationArgs {
	a.Set[LAST] = true
	a.last = last
	a.Map[LAST] = last

	return a
}
func (a *ExTsAggregationArgs) Range(tsRange int64) *ExTsAggregationArgs {
	a.Set[RANGE] = true
	a.aggRange = tsRange
	a.Map[RANGE] = tsRange
	return a
}

func (a *ExTsAggregationArgs) GetRangeArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[MAXCOUNT]; ok {
		args = append(args, MAXCOUNT, a.maxCount)
	}
	if _, ok := a.Set[REVERSE]; ok {
		args = append(args, REVERSE)
	}
	for _, menu := range MENUS {
		if _, ok := a.Set[menu]; ok {
			args = append(args, AGGREGATION, menu)
			args = append(args, a.Map[menu])
			break
		}
	}
	return args
}

func (a *ExTsAggregationArgs) GetSRangeArgs(filters []*ExTsFilter) []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[MAXCOUNT]; ok {
		args = append(args, MAXCOUNT)
	}
	for _, menu := range MENUS {
		if _, ok := a.Set[menu]; ok {
			args = append(args, AGGREGATION, menu, a.Map[menu])
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

	for _, f := range filters {
		args = append(args, f.Filter())
	}

	return args
}

func (a *ExTsAggregationArgs) GetMRangeArgs(pKey string, sKeys []string, startTs string, endTs string) []interface{} {
	args := make([]interface{}, 0)
	args = append(args, pKey)
	args = append(args, len(sKeys))
	for _, sKey := range sKeys {
		args = append(args, sKey)
	}
	args = append(args, startTs, endTs)
	if _, ok := a.Set[MAXCOUNT]; ok {
		args = append(args, MAXCOUNT, a.Map[MAXCOUNT])
	}
	if _, ok := a.Set[WITHLABELS]; ok {
		args = append(args, WITHLABELS)
	}
	if _, ok := a.Set[REVERSE]; ok {
		args = append(args, REVERSE)
	}
	for _, menu := range MENUS {
		if _, ok := a.Set[menu]; ok {
			args = append(args, AGGREGATION, menu, a.Map[menu])
			break
		}
	}
	return args
}

func (a *ExTsAggregationArgs) GetMRangeFilter(filters []*ExTsFilter) []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[MAXCOUNT]; ok {
		args = append(args, MAXCOUNT, a.Map[MAXCOUNT])
	}
	for _, menu := range MENUS {
		if _, ok := a.Set[menu]; ok {
			args = append(args, AGGREGATION, menu, a.Map[menu])
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
	for _, menu := range MENUS {
		if _, ok := a.Set[menu]; ok {
			args = append(args, AGGREGATION, menu)
			args = append(args, a.Map[menu])
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

func (tc tairCmdable) TsPCreate(ctx context.Context, key string) *redis.StringCmd {
	args := make([]interface{}, 2)
	args[0] = "EXTS.P.CREATE"
	args[1] = key
	cmd := redis.NewStringCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) TsSCreate(ctx context.Context, key string, value interface{}, args *ExTsAttributeArgs) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "EXTS.S.CREATE"
	a[1] = key
	a[2] = value
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
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

func (tc tairCmdable) ExTsMAdd(ctx context.Context, pKey string, sKeys []*ExTsDataPoint) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.MADD"
	args := ExTsAttributeArgs{}.New()
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMAddArgs(ctx context.Context, pKey string, sKeys []*ExTsDataPoint, args *ExTsAttributeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.MADD"
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsAlter(ctx context.Context, pKey, sKey string, args *ExTsAttributeArgs) *redis.StringCmd {
	a := make([]interface{}, 3)
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

func (tc tairCmdable) ExTsMIncr(ctx context.Context, pKey string, sKeys []*ExTsDataPoint) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.MINCRBY"
	args := ExTsAttributeArgs{}.New()
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMIncrArgs(ctx context.Context, pKey string, sKeys []*ExTsDataPoint, args *ExTsAttributeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.MINCRBY"
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsDel(ctx context.Context, pKey string, sKeys string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "EXTS.S.DEL"
	a[1] = pKey
	a[2] = sKeys
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsGet(ctx context.Context, pKey string, sKeys string) *ExTsDataPointCmd {
	a := make([]interface{}, 3)
	a[0] = "EXTS.S.GET"
	a[1] = pKey
	a[2] = sKeys
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := NewExTsDataPointCmd(cmd)
	return resCmd
}

func (tc tairCmdable) ExTsQuery(ctx context.Context, pKey string, filters []*ExTsFilter) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.QUERYINDEX"
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
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := NewExTsSKeyCmd(cmd)
	return resCmd
}

func (tc tairCmdable) ExTsRangeArgs(ctx context.Context, pKey string, sKey string, startTs string, endTs string, args *ExTsAggregationArgs) *ExTsSKeyCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.RANGE"
	a[1] = pKey
	a[2] = sKey
	a[3] = startTs
	a[4] = endTs
	a = append(a, args.GetRangeArgs()...)
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := &ExTsSKeyCmd{}
	resCmd.SliceCmd = cmd
	resCmd.BuildForExTsRange()
	return resCmd
}

func (tc tairCmdable) ExTsMRange(ctx context.Context, pKey string, startTs string, endTs string, filters []*ExTsFilter) *ExTsSKeySliceCmd {
	a := make([]interface{}, 4)
	a[0] = "EXTS.S.MRANGE"
	a[1] = pKey
	a[2] = startTs
	a[3] = endTs
	args := ExTsAggregationArgs{}.New()
	a = append(a, args.GetMRangeFilter(filters)...)
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := NewExTsSKeySliceCmd(cmd)
	return resCmd
}

func (tc tairCmdable) ExTsMRangeFilter(ctx context.Context, pKey string, startTs string, endTs string, filters []*ExTsFilter) *ExTsSKeySliceCmd {
	a := make([]interface{}, 4)
	a[0] = "EXTS.S.MRANGE"
	a[1] = pKey
	a[2] = startTs
	a[3] = endTs
	args := ExTsAggregationArgs{}.New()
	a = append(a, args.GetMRangeFilter(filters)...)
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := NewExTsSKeySliceCmd(cmd)
	return resCmd
}

func (tc tairCmdable) ExTsMRangeFilterArgs(ctx context.Context, pKey string, startTs string, endTs string, filters []*ExTsFilter, args *ExTsAggregationArgs) *ExTsSKeySliceCmd {
	a := make([]interface{}, 4)
	a[0] = "EXTS.S.MRANGE"
	a[1] = pKey
	a[2] = startTs
	a[3] = endTs
	a = append(a, args.GetMRangeFilter(filters)...)
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := NewExTsSKeySliceCmd(cmd)
	return resCmd
}

func (tc tairCmdable) ExTsPRange(ctx context.Context, pKey string, startTs string, endTs string, pkeyAggregationType string, pkeyTimeBucket int64, filters []*ExTsFilter) *ExTsSKeyCmd {
	a := make([]interface{}, 6)
	a[0] = "EXTS.P.RANGE"
	a[1] = pKey
	a[2] = startTs
	a[3] = endTs
	a[4] = pkeyAggregationType
	a[5] = pkeyTimeBucket
	args := ExTsAggregationArgs{}.New()
	a = append(a, args.GetPRangeArgs(filters)...)
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := NewExTsSKeyCmd(cmd)
	return resCmd
}

func (tc tairCmdable) ExTsPRangeArgs(ctx context.Context, pKey string, startTs string, endTs string, pkeyAggregationType string, pkeyTimeBucket int64, filters []*ExTsFilter, args *ExTsAggregationArgs) *ExTsSKeyCmd {
	a := make([]interface{}, 6)
	a[0] = "EXTS.P.RANGE"
	a[1] = pKey
	a[2] = startTs
	a[3] = endTs
	a[4] = pkeyAggregationType
	a[5] = pkeyTimeBucket
	a = append(a, args.GetPRangeArgs(filters)...)
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := NewExTsSKeyCmd(cmd)
	return resCmd
}

func (tc tairCmdable) ExTsRawModify(ctx context.Context, pKey string, sKey string, ts string, value float64) *redis.StringCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.RAW_MODIFY"
	a[1] = pKey
	a[2] = sKey
	a[3] = ts
	a[4] = value
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRawModifyArgs(ctx context.Context, pKey string, sKey string, ts string, value float64, args *ExTsAttributeArgs) *redis.StringCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.RAW_MODIFY"
	a[1] = pKey
	a[2] = sKey
	a[3] = ts
	a[4] = value
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRawModify(ctx context.Context, pKey string, sKeys []*ExTsDataPoint) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.RAW_MMODIFY"
	args := ExTsMAddArgs{}.New()
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRawModifyArgs(ctx context.Context, pKey string, sKeys []*ExTsDataPoint, args *ExTsAttributeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.RAW_MMODIFY"
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRawIncr(ctx context.Context, pKey string, sKey string, ts string, value float64) *redis.StringCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.RAW_MODIFY"
	a[1] = pKey
	a[2] = sKey
	a[3] = ts
	a[4] = value
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsRawIncrArgs(ctx context.Context, pKey string, sKey string, ts string, value float64, args *ExTsAttributeArgs) *redis.StringCmd {
	a := make([]interface{}, 5)
	a[0] = "EXTS.S.RAW_MODIFY"
	a[1] = pKey
	a[2] = sKey
	a[3] = ts
	a[4] = value
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRawIncr(ctx context.Context, pKey string, sKeys []*ExTsDataPoint) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.RAW_MINCRBY"
	args := ExTsMAddArgs{}.New()
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExTsMRawIncrArgs(ctx context.Context, pKey string, sKeys []*ExTsDataPoint, args *ExTsAttributeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 1)
	a[0] = "EXTS.S.RAW_MINCRBY"
	a = append(a, args.JoinArgs(pKey, sKeys)...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
