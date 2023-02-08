package tair

import (
	"context"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type CpcData struct {
	arg
	key       string
	item      string
	expStr    string
	exp       int64
	hasSetExp bool
}

func (a *CpcData) ExpStr() string {
	return a.expStr
}

func (a *CpcData) Exp() int64 {
	return a.exp
}

func (a *CpcData) Key() string {
	return a.key
}

func (a *CpcData) SetKey(key string) {
	a.key = key
}

func (a *CpcData) Item() string {
	return a.item
}

func (a *CpcData) SetItem(item string) {
	a.item = item
}

func (a *CpcData) New() *CpcData {
	a.Set = make(map[string]bool)
	return a
}

func (a *CpcData) ex(secondsToExpire int64) *CpcData {
	if a.hasSetExp {
		panic(ExpIsSet)
	}
	a.hasSetExp = true
	a.Set[EX] = true
	a.exp = secondsToExpire
	return a
}

func (a *CpcData) px(millisecondsToExpire int64) *CpcData {
	if a.hasSetExp {
		panic(ExpIsSet)
	}
	a.hasSetExp = true
	a.Set[PX] = true
	a.exp = millisecondsToExpire
	return a
}

func (a *CpcData) exAt(secondsToExpire int64) *CpcData {
	if a.hasSetExp {
		panic(ExpIsSet)
	}
	a.hasSetExp = true
	a.Set[EXAT] = true
	a.exp = secondsToExpire
	return a
}

func (a *CpcData) pxAt(millisecondsToExpire int64) *CpcData {
	if a.hasSetExp {
		panic(ExpIsSet)
	}
	a.hasSetExp = true
	a.Set[PXAT] = true
	a.exp = millisecondsToExpire
	return a
}

type CpcUpdateArgs struct {
	arg
	ex      time.Duration
	exAt    time.Time
	px      time.Duration
	pxAt    time.Time
	size    int64
	winSize int64
}

func (a CpcUpdateArgs) New() *CpcUpdateArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *CpcUpdateArgs) Ex() time.Duration {
	return a.ex
}

func (a *CpcUpdateArgs) SetEx(ex time.Duration) {
	a.Set[EX] = true
	a.ex = ex
}

func (a *CpcUpdateArgs) ExAt() time.Time {
	return a.exAt
}

func (a *CpcUpdateArgs) SetExAt(exAt time.Time) {
	a.Set[EXAT] = true
	a.exAt = exAt
}

func (a *CpcUpdateArgs) Px() time.Duration {
	return a.px
}

func (a *CpcUpdateArgs) SetPx(px time.Duration) {
	a.Set[PX] = true
	a.px = px
}

func (a *CpcUpdateArgs) PxAt() time.Time {
	return a.pxAt
}

func (a *CpcUpdateArgs) SetPxAt(pxAt time.Time) {
	a.Set[PXAT] = true
	a.pxAt = pxAt
}

func (a *CpcUpdateArgs) Size() int64 {
	return a.size
}

func (a *CpcUpdateArgs) SetSize(size int64) {
	a.Set[SIZE] = true
	a.size = size
}

func (a *CpcUpdateArgs) WinSize() int64 {
	return a.winSize
}

func (a *CpcUpdateArgs) SetWinSize(winSize int64) {
	a.Set[WIN] = true
	a.winSize = winSize
}

func (a *CpcUpdateArgs) GetArgs() []interface{} {
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
	if _, ok := a.Set[WIN]; ok {
		args = append(args, WIN, a.winSize)
	}
	if _, ok := a.Set[SIZE]; ok {
		args = append(args, SIZE, a.size)
	}
	return args
}

type CpcMultiUpdateArgs struct {
	arg
}

func (a CpcMultiUpdateArgs) New() *CpcMultiUpdateArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a CpcMultiUpdateArgs) JoinArgs(cpcData []CpcData) []interface{} {
	args := make([]interface{}, 0)
	for _, data := range cpcData {
		args = append(args, data.Key(), data.Item(), data.ExpStr(), data.Exp())
	}
	return args
}

type Update2JudCmd struct {
	*redis.SliceCmd
	value     float64
	diffValue float64
}

func (cmd *Update2JudCmd) Value() float64 {
	val := cmd.SliceCmd.Val()
	var err error
	cmd.value, err = strconv.ParseFloat(val[0].(string), 64)
	if err != nil {
		panic("cannot parse float")
	}
	return cmd.value
}

func (cmd *Update2JudCmd) SetValue(value float64) {
	cmd.value = value
}

func (cmd *Update2JudCmd) DiffValue() float64 {
	val := cmd.SliceCmd.Val()
	var err error
	cmd.diffValue, err = strconv.ParseFloat(val[1].(string), 64)
	if err != nil {
		panic("cannot parse float")
	}
	return cmd.diffValue
}

func (cmd *Update2JudCmd) SetDiffValue(diffValue float64) {
	cmd.diffValue = diffValue
}

func (cmd *Update2JudCmd) Result() (*Update2JudCmd, error) {
	return cmd, cmd.SliceCmd.Err()
}

func NewUpdate2JudCmd(ctx context.Context, arg ...interface{}) *Update2JudCmd {
	return &Update2JudCmd{
		SliceCmd: redis.NewSliceCmd(ctx, arg...),
	}
}

func (tc tairCmdable) CpcUpdate(ctx context.Context, key string, item string) *redis.StringCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 3)
	a[0] = "CPC.UPDATE"
	a[1] = key
	a[2] = item
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcEstimate(ctx context.Context, key string) *redis.FloatCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	a := make([]interface{}, 2)
	a[0] = "CPC.ESTIMATE"
	a[1] = key
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcUpdateArgs(ctx context.Context, key string, item string, args *CpcUpdateArgs) *redis.StringCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 3)
	a[0] = "CPC.UPDATE"
	a[1] = key
	a[2] = item
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcUpdate2Est(ctx context.Context, key string, item string) *redis.FloatCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 3)
	a[0] = "CPC.UPDATE2EST"
	a[1] = key
	a[2] = item
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcUpdate2EstArgs(ctx context.Context, key string, item string, args CpcUpdateArgs) *redis.FloatCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 3)
	a[0] = "CPC.UPDATE2EST"
	a[1] = key
	a[2] = item
	a = append(a, args.GetArgs()...)
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcUpdate2Jud(ctx context.Context, key string, item string) *Update2JudCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 3)
	a[0] = "CPC.UPDATE2JUD"
	a[1] = key
	a[2] = item
	cmd := NewUpdate2JudCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcUpdate2JudArgs(ctx context.Context, key string, item string, args CpcUpdateArgs) *Update2JudCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 3)
	a[0] = "CPC.UPDATE"
	a[1] = key
	a[2] = item
	a = append(a, args.GetArgs()...)
	cmd := NewUpdate2JudCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

// array
func (tc tairCmdable) CpcArrayUpdate(ctx context.Context, key string, timestamp int64, item string) *redis.StringCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 4)
	a[0] = "CPC.ARRAY.UPDATE"
	a[1] = key
	a[2] = timestamp
	a[3] = item
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcArrayUpdateArgs(ctx context.Context, key string, timestamp int64, item string, args CpcUpdateArgs) *redis.FloatCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 4)
	a[0] = "CPC.ARRAY.UPDATE"
	a[1] = key
	a[2] = timestamp
	a[3] = item
	a = append(a, args.GetArgs()...)
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcArrayEstimate(ctx context.Context, key string, timestamp int64) *redis.FloatCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	a := make([]interface{}, 3)
	a[0] = "CPC.ARRAY.ESTIMATE"
	a[1] = key
	a[2] = timestamp
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcArrayEstimateRange(ctx context.Context, key string, startTime int64, endTime int64) *redis.FloatSliceCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	a := make([]interface{}, 4)
	a[0] = "CPC.ARRAY.ESTIMATE.RANGE"
	a[1] = key
	a[2] = startTime
	a[3] = endTime
	cmd := redis.NewFloatSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcArrayEstimateRangeMerge(ctx context.Context, key string, timestamp int64, estRange int64) *redis.FloatCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	a := make([]interface{}, 4)
	a[0] = "CPC.ARRAY.ESTIMATE.RANGE.SUM"
	a[1] = key
	a[2] = timestamp
	a[3] = estRange
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcArrayUpdate2Est(ctx context.Context, key string, timestamp int64, item string) *redis.FloatCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 4)
	a[0] = "CPC.ARRAY.UPDATE2EST"
	a[1] = key
	a[2] = timestamp
	a[3] = item
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcArrayUpdate2EstArgs(ctx context.Context, key string, timestamp int64, item string, args CpcUpdateArgs) *redis.FloatCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 4)
	a[0] = "CPC.ARRAY.UPDATE2EST"
	a[1] = key
	a[2] = timestamp
	a[3] = item
	a = append(a, args.GetArgs()...)
	cmd := redis.NewFloatCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcArrayUpdate2Jud(ctx context.Context, key string, timestamp int64, item string) *Update2JudCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 4)
	a[0] = "CPC.ARRAY.UPDATE2JUD"
	a[1] = key
	a[2] = timestamp
	a[3] = item
	cmd := NewUpdate2JudCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) CpcArrayUpdate2JudArgs(ctx context.Context, key string, timestamp int64, item string, args CpcUpdateArgs) *Update2JudCmd {
	if key == "" {
		panic(KeyIsEmpty)
	}
	if item == "" {
		panic(ValueIsEmpty)
	}
	a := make([]interface{}, 4)
	a[0] = "CPC.ARRAY.UPDATE2JUD"
	a[1] = key
	a[2] = timestamp
	a[3] = item
	a = append(a, args.GetArgs()...)
	cmd := NewUpdate2JudCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

// end
