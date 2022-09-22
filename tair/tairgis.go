package tair

import (
	"context"
	"github.com/go-redis/redis/v8"
	"strconv"
)

// args

type GisArgs struct {
	arg
	radius       string
	member       string
	withoutWkt   string
	withValue    string
	withoutValue string
	withDist     string
	asc          string
	desc         string
	count        int
}

func (a GisArgs) NewGisArgs() *GisArgs {
	a.Set = make(map[string]bool)
	return &a
}

func (a *GisArgs) Radius() *GisArgs {
	a.Set[RADIUS] = true
	return a
}

func (a *GisArgs) Member() *GisArgs {
	a.Set[MEMBER] = true
	return a
}

func (a *GisArgs) WithoutWkt() *GisArgs {
	a.Set[WITHOUTWKT] = true
	return a
}

func (a *GisArgs) WithValue() *GisArgs {
	a.Set[WITHVALUE] = true
	return a
}

func (a *GisArgs) WithoutValue() *GisArgs {
	a.Set[WITHOUTVALUE] = true
	return a
}

func (a *GisArgs) WithDist() *GisArgs {
	a.Set[WITHDIST] = true
	return a
}

func (a *GisArgs) Asc() *GisArgs {
	a.Set[ASC] = true
	return a
}

func (a *GisArgs) Desc() *GisArgs {
	a.Set[DESC] = true
	return a
}

func (a *GisArgs) Count(count int) *GisArgs {
	a.Set[COUNT] = true
	a.count = count
	return a
}

func (a *GisArgs) GetArgs() []interface{} {
	args := make([]interface{}, 0)
	if _, ok := a.Set[RADIUS]; ok {
		args = append(args, RADIUS)
	}
	if _, ok := a.Set[MEMBER]; ok {
		args = append(args, MEMBER)
	}
	if _, ok := a.Set[WITHOUTWKT]; ok {
		args = append(args, WITHOUTWKT)
	}
	if _, ok := a.Set[WITHVALUE]; ok {
		args = append(args, WITHVALUE)
	}
	if _, ok := a.Set[WITHOUTVALUE]; ok {
		args = append(args, WITHOUTVALUE)
	}
	if _, ok := a.Set[WITHDIST]; ok {
		args = append(args, WITHDIST)
	}
	if _, ok := a.Set[ASC]; ok {
		args = append(args, ASC)
	}
	if _, ok := a.Set[DESC]; ok {
		args = append(args, DESC)
	}
	if _, ok := a.Set[COUNT]; ok {
		args = append(args, COUNT, a.count)
	}
	return args

}

// cmd

type GisSearchResult struct {
	field    []byte
	value    []byte
	distance float64
}

func (cmd *GisSearchResult) Field() []byte {
	return cmd.field
}

func (cmd *GisSearchResult) Value() []byte {
	return cmd.value
}

func (cmd *GisSearchResult) FieldByString() string {
	if cmd.field == nil {
		return ""
	}
	return string(cmd.field[:])
}

func (cmd *GisSearchResult) ValueByString() string {
	if cmd.value == nil {
		return ""
	}
	return string(cmd.value[:])
}

func (cmd *GisSearchResult) Distance() float64 {
	return cmd.distance
}

//func (cmd *GisSearchResult) Build() *GisSearchResult {
//	panic("im")
//}
//
//func (cmd *GisSearchResult) BuildFromVal(c interface{}) *GisSearchResult {
//	val := c.([]interface{})
//	cmd.field = val[0].([]byte)
//	cmd.value = val[1].([]byte)
//	cmd.distance = val[2].(float64)
//	return cmd
//}

type GisSearchSliceMapCmd struct {
	*redis.SliceCmd
	*GisSearchResult
}

func NewGisSearchSliceMapCmd(sliceCmd *redis.SliceCmd) *GisSearchSliceMapCmd {
	cmd := &GisSearchSliceMapCmd{}
	cmd.SliceCmd = sliceCmd
	return cmd
}

func (cmd *GisSearchSliceMapCmd) Result() ([]*GisSearchResult, error) {
	val, err := cmd.SliceCmd.Result()
	if len(val) == 0 || err != nil {
		return make([]*GisSearchResult, 0), err
	} else {
		num := val[0].(int64)
		rawRes := val[1].([]interface{})
		size := (len(rawRes)) / int(num)

		m := make([]*GisSearchResult, 0)
		for i := 0; i < int(num); i = i + 1 {
			result := &GisSearchResult{}
			result.field = []byte(rawRes[i*size].(string))
			for j := i*size + 1; j < (i+1)*size; j++ {
				if floatVar, err := strconv.ParseFloat(rawRes[j].(string), 64); err == nil {
					result.distance = floatVar
				} else {
					result.value = []byte(rawRes[j].(string))
				}
			}
			m = append(m, result)
		}
		return m, nil
	}
}

//func (cmd *GisSearchSliceMapCmd) String() string {
//
//}

type GisStringStringMapCmd struct {
	*redis.SliceCmd
}

func NewGisStringStringMapCmd(sliceCmd *redis.SliceCmd) *GisStringStringMapCmd {
	cmd := &GisStringStringMapCmd{
		SliceCmd: sliceCmd,
	}
	return cmd
}

func (cmd *GisStringStringMapCmd) Result() (map[string]string, error) {
	val, err := cmd.SliceCmd.Result()
	if len(val) == 0 || err != nil {
		return make(map[string]string, 0), err
	} else {
		rawRes := val[1].([]interface{})
		m := make(map[string]string, 0)
		for i := 0; i < len(rawRes); i = i + 2 {
			m[rawRes[i].(string)] = rawRes[i+1].(string)
		}
		return m, nil
	}
}

type GisGetAllStringStringMapCmd struct {
	*redis.SliceCmd
}

func NewGisGetAllStringStringMapCmd(sliceCmd *redis.SliceCmd) *GisGetAllStringStringMapCmd {
	cmd := &GisGetAllStringStringMapCmd{
		SliceCmd: sliceCmd,
	}
	return cmd
}

func (cmd *GisGetAllStringStringMapCmd) Result() (map[string]string, error) {
	val, err := cmd.SliceCmd.Result()
	if len(val) == 0 || err != nil {
		return make(map[string]string, 0), err
	} else {
		m := make(map[string]string, 0)
		for i := 0; i < len(val); i = i + 2 {
			m[val[i].(string)] = val[i+1].(string)
		}
		return m, nil
	}
}

type GisGetAllStringCmd struct {
	*redis.SliceCmd
}

func NewGisGetAllStringCmd(sliceCmd *redis.SliceCmd) *GisGetAllStringCmd {
	cmd := &GisGetAllStringCmd{
		SliceCmd: sliceCmd,
	}
	return cmd
}

func (cmd *GisGetAllStringCmd) Result() ([]string, error) {
	val, err := cmd.SliceCmd.Result()
	if len(val) == 0 || err != nil {
		return make([]string, 0), err
	} else {
		m := make([]string, 0)
		for _, item := range val {
			if item == nil {
				m = append(m, "")
			} else {
				m = append(m, item.(string))
			}
		}
		return m, nil
	}
}

type GisContainsStringCmd struct {
	*redis.SliceCmd
}

func NewGisContainsStringCmd(sliceCmd *redis.SliceCmd) *GisContainsStringCmd {
	cmd := &GisContainsStringCmd{
		SliceCmd: sliceCmd,
	}
	return cmd
}

func (cmd *GisContainsStringCmd) Result() ([]string, error) {
	val, err := cmd.SliceCmd.Result()
	if len(val) == 0 || err != nil {
		return make([]string, 0), err
	} else {
		rawRes := val[1].([]interface{})
		m := make([]string, 0)
		for _, item := range rawRes {
			if item == nil {
				m = append(m, "")
			} else {
				m = append(m, item.(string))
			}
		}
		return m, nil
	}
}

func (tc tairCmdable) GisAdd(ctx context.Context, area string, polygonName string, polygonWktText string) *redis.IntCmd {
	args := make([]interface{}, 4)
	args[0] = "GIS.ADD"
	args[1] = area
	args[2] = polygonName
	args[3] = polygonWktText
	cmd := redis.NewIntCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) GisGet(ctx context.Context, area string, polygonName string) *redis.StringCmd {
	args := make([]interface{}, 3)
	args[0] = "GIS.GET"
	args[1] = area
	args[2] = polygonName
	cmd := redis.NewStringCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) GisSearch(ctx context.Context, area string, pointWktText string) *GisStringStringMapCmd {
	args := make([]interface{}, 3)
	args[0] = "GIS.SEARCH"
	args[1] = area
	args[2] = pointWktText
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	resCmd := NewGisStringStringMapCmd(cmd)
	return resCmd
}

func (tc tairCmdable) GisSearchArgs(ctx context.Context, area string, longitude, latitude, radius float64,
	geoUnit string, args *GisArgs) *GisSearchSliceMapCmd {
	a := make([]interface{}, 7)
	a[0] = "GIS.SEARCH"
	a[1] = area
	a[2] = RADIUS
	a[3] = longitude
	a[4] = latitude
	a[5] = radius
	a[6] = geoUnit
	a = append(a, args.GetArgs()...)
	cmd := redis.NewSliceCmd(ctx, a...)
	resCmd := NewGisSearchSliceMapCmd(cmd)
	_ = tc(ctx, cmd)
	return resCmd
}

func (tc tairCmdable) GisSearchArgsByMember(ctx context.Context, area string, member string, radius float64, geoUnit string, args *GisArgs) *GisSearchSliceMapCmd {
	a := make([]interface{}, 6)
	a[0] = "GIS.SEARCH"
	a[1] = area
	a[2] = MEMBER
	a[3] = member
	a[4] = radius
	a[5] = geoUnit
	a = append(a, args.GetArgs()...)
	cmd := redis.NewSliceCmd(ctx, a...)
	resCmd := NewGisSearchSliceMapCmd(cmd)
	_ = tc(ctx, cmd)
	return resCmd
}

func (tc tairCmdable) GisContains(ctx context.Context, area string, pointWktText string) *GisStringStringMapCmd {
	args := make([]interface{}, 3)
	args[0] = "GIS.CONTAINS"
	args[1] = area
	args[2] = pointWktText
	cmd := redis.NewSliceCmd(ctx, args...)
	resCmd := NewGisStringStringMapCmd(cmd)
	_ = tc(ctx, cmd)
	return resCmd
}

func (tc tairCmdable) GisContainsArgs(ctx context.Context, area string, pointWktText string, args *GisArgs) *GisContainsStringCmd {
	a := make([]interface{}, 3)
	a[0] = "GIS.CONTAINS"
	a[1] = area
	a[2] = pointWktText
	a = append(a, args.GetArgs()...)
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := NewGisContainsStringCmd(cmd)
	return resCmd
}

func (tc tairCmdable) GisIntersects(ctx context.Context, area string, pointWktText string) *GisStringStringMapCmd {
	args := make([]interface{}, 3)
	args[0] = "GIS.INTERSECTS"
	args[1] = area
	args[2] = pointWktText
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	resCmd := NewGisStringStringMapCmd(cmd)
	return resCmd
}

func (tc tairCmdable) GisDel(ctx context.Context, area string, polygonName string) *redis.StringCmd {
	args := make([]interface{}, 3)
	args[0] = "GIS.DEL"
	args[1] = area
	args[2] = polygonName
	cmd := redis.NewStringCmd(ctx, args...)
	_ = tc(ctx, cmd)
	return cmd
}

func (tc tairCmdable) GisGetAll(ctx context.Context, area string) *GisGetAllStringStringMapCmd {
	args := make([]interface{}, 2)
	args[0] = "GIS.GETALL"
	args[1] = area
	cmd := redis.NewSliceCmd(ctx, args...)
	_ = tc(ctx, cmd)
	resCmd := NewGisGetAllStringStringMapCmd(cmd)
	return resCmd
}

func (tc tairCmdable) GisGetAllArgs(ctx context.Context, area string, args *GisArgs) *GisGetAllStringCmd {
	a := make([]interface{}, 2)
	a[0] = "GIS.GETALL"
	a[1] = area
	a = append(a, args.GetArgs()...)
	cmd := redis.NewSliceCmd(ctx, a...)
	_ = tc(ctx, cmd)
	resCmd := NewGisGetAllStringCmd(cmd)
	return resCmd
}

func (tc tairCmdable) GisWithin(ctx context.Context, area string, polygonWkt string, withoutWkt bool) *redis.StringStringMapCmd {
	a := make([]interface{}, 3)
	a[0] = "GIS.WITHIN"
	a[1] = area
	a[2] = polygonWkt
	if withoutWkt {
		a = append(a, "WITHOUTWKT")
	}
	cmd := redis.NewStringStringMapCmd(ctx, a...)
	_ = tc(ctx, cmd)
	return cmd
}
