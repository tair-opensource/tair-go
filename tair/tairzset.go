package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strings"
)

type ExZAddArgs struct {
	arg
	xx   string
	nx   string
	ch   string
	incr string
}

func (a ExZAddArgs) New() *ExZAddArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExZAddArgs) Xx() *ExZAddArgs {
	a.Set[XX] = true
	return a
}

func (a *ExZAddArgs) Nx() *ExZAddArgs {
	a.Set[NX] = true
	return a
}

func (a *ExZAddArgs) Ch() *ExZAddArgs {
	a.Set[CH] = true
	return a
}

func (a *ExZAddArgs) Incr() *ExZAddArgs {
	a.Set[INCR] = true
	return a
}

func (p *ExZAddArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := p.Set[XX]; ok {
		args = append(args, XX)
	}
	if _, ok := p.Set[NX]; ok {
		args = append(args, NX)
	}
	if _, ok := p.Set[CH]; ok {
		args = append(args, CH)
	}
	if _, ok := p.Set[INCR]; ok {
		args = append(args, INCR)
	}
	return args
}

type ExZAddMember struct {
	Score  string
	Member string
}

func joinScoresToString(scores ...string) string {
	var builder strings.Builder
	for _, score := range scores {
		builder.WriteString(score)
		builder.WriteString("#")
	}
	strs := builder.String()
	return strs[:len(strs)-1]
}

type ExZRangeArgs struct {
	arg
	arger
	withScores string
	offset     int64
	count      int64
}

func (a ExZRangeArgs) New() *ExZRangeArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *ExZRangeArgs) WithScores() *ExZRangeArgs {
	a.Set[WITHSCORES] = true
	return a
}

func (a *ExZRangeArgs) Limit(offset, count int64) *ExZRangeArgs {
	a.Set[LIMIT] = true
	a.offset = offset
	a.count = count
	return a
}

func (a *ExZRangeArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[WITHSCORES]; ok {
		args = append(args, WITHSCORES)
	}
	if _, ok := a.Set[LIMIT]; ok {
		args = append(args, LIMIT, a.offset, a.count)
	}
	return args
}

func (tc tairCmdable) exZAdd(ctx context.Context, key string, p *ExZAddArgs, member ...ExZAddMember) *redis.IntCmd {
	a := make([]interface{}, 0)
	a = append(a, "exzadd", key)

	a = append(a, p.GetArgs()...)
	for _, m := range member {
		a = append(a, m.Score)
		a = append(a, m.Member)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd

}
func (tc tairCmdable) ExZAddManyScore(ctx context.Context, key string, member string, scores ...string) *redis.IntCmd {
	args := make([]interface{}, 4)
	args[0] = "exzadd"
	args[1] = key
	args[2] = joinScoresToString(scores...)
	args[3] = member
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZAdd(ctx context.Context, key string, score string, member string) *redis.IntCmd {
	cmd := tc.exZAdd(ctx, key, ExZAddArgs{}.New(), ExZAddMember{score, member})
	return cmd
}

func (tc tairCmdable) ExZAddArgs(ctx context.Context, key string, score string, member string, a *ExZAddArgs) *redis.IntCmd {
	cmd := tc.exZAdd(ctx, key, a, ExZAddMember{score, member})
	return cmd
}

func (tc tairCmdable) ExZAddManyMember(ctx context.Context, key string, member ...ExZAddMember) *redis.IntCmd {
	cmd := tc.exZAdd(ctx, key, ExZAddArgs{}.New(), member...)
	return cmd
}

func (tc tairCmdable) ExZAddManyMemberArgs(ctx context.Context, key string, a *ExZAddArgs, member ...ExZAddMember) *redis.IntCmd {
	cmd := tc.exZAdd(ctx, key, a, member...)
	return cmd
}

func (tc tairCmdable) ExZIncrBy(ctx context.Context, key string, score string, member string) *redis.StringCmd {
	a := make([]interface{}, 4)
	a[0] = "exzincrby"
	a[1] = key
	a[2] = score
	a[3] = member
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZIncrByManyScore(ctx context.Context, key string, member string, score ...string) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzincryby"
	a[1] = key
	a[2] = joinScoresToString(score...)
	a[3] = member
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) ExZRem(ctx context.Context, key string, member ...string) *redis.IntCmd {
	a := make([]interface{}, 2)
	a[0] = "exzrem"
	a[1] = key
	for _, m := range member { // todo slice copy performance optimization
		a = append(a, m)
	}
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exzremrangebyscore"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRemRangeByRank(ctx context.Context, key string, start, stop int) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exzremrangebyrank"
	a[1] = key
	a[2] = start
	a[3] = stop
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRemRangeByLex(ctx context.Context, key, min, max string) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exzremrangebylex"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZScore(ctx context.Context, key, member string) *redis.StringCmd {
	a := make([]interface{}, 3)
	a[0] = "exzscore"
	a[1] = key
	a[2] = member
	cmd := redis.NewStringCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRange(ctx context.Context, key string, min, max int64) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrange"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRangeWithScores(ctx context.Context, key string, min, max int64) *redis.StringSliceCmd {
	a := make([]interface{}, 5)
	a[0] = "exzrange"
	a[1] = key
	a[2] = min
	a[3] = max
	a[4] = "WITHSCORES"
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRevRange(ctx context.Context, key string, min, max int) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrevrange"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRevRangeWithScores(ctx context.Context, key string, min, max int64) *redis.StringSliceCmd {
	a := make([]interface{}, 5)
	a[0] = "exzrevrange"
	a[1] = key
	a[2] = min
	a[3] = max
	a[4] = "WITHSCORES"
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRangeByScore(ctx context.Context, key, min, max string) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrangebyscore"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRangeByScoreWithArgs(ctx context.Context, key, min, max string, arg *ExZRangeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrangebyscore"
	a[1] = key
	a[2] = min
	a[3] = max
	a = append(a, arg.GetArgs()...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRevRangeByScore(ctx context.Context, key, min, max string) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrevrangebyscore"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRevRangeByScoreWithArgs(ctx context.Context, key, min, max string, arg *ExZRangeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrevrangebyscore"
	a[1] = key
	a[2] = min
	a[3] = max
	a = append(a, arg.GetArgs()...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRangeByLex(ctx context.Context, key, min, max string) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrangebylex"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRangeByLexWithArgs(ctx context.Context, key, min, max string, args *ExZRangeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrangebylex"
	a[1] = key
	a[2] = min
	a[3] = max
	a = append(a, args.GetArgs()...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRevRangeByLex(ctx context.Context, key, min, max string) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrevrangebylex"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRevRangeByLexWithArgs(ctx context.Context, key, min, max string, arg *ExZRangeArgs) *redis.StringSliceCmd {
	a := make([]interface{}, 4)
	a[0] = "exzrevrangebylex"
	a[1] = key
	a[2] = min
	a[3] = max
	a = append(a, arg.GetArgs()...)
	cmd := redis.NewStringSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) ExZCard(ctx context.Context, key string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "exzcard", key)
	_ = tc(ctx, cmd)
	return cmd
}
func (tc tairCmdable) ExZRank(ctx context.Context, key, member string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "exzrank", key, member)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRevRank(ctx context.Context, key, member string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "exzrevrank", key, member)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRankByScore(ctx context.Context, key, score string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "exzrankbyscore", key, score)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZRevRankByScore(ctx context.Context, key, score string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx, "exzrevrankbyscore", key, score)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exzcount"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) ExZLexCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	a := make([]interface{}, 4)
	a[0] = "exzlexcount"
	a[1] = key
	a[2] = min
	a[3] = max
	cmd := redis.NewIntCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
