package main

import (
	"context"

	"github.com/alibaba/tair-go/tair"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

var tairClient *tair.TairClient

func init() {
	tairClient = tair.NewTairClient(&redis.Options{
		Addr:     "***.redis.rds.aliyuncs.com:6379",
		Password: "xxx",
		DB:       0,
	})
}

type CpuCurve struct {
	// field
}

func (l *CpuCurve) addPoint(ip, ts string, value float64) bool {
	result, err := tairClient.ExTsAdd(ctx, "CPU_LOAD", ip, ts, value).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	if "OK" == result {
		return true
	}
	return false
}

func (l *CpuCurve) rangePoint(ip, startTs, endTs string) *tair.ExTsSKeyCmd {
	result, err := tairClient.ExTsRange(ctx, "CPU_LOAD", ip, startTs, endTs).Result()
	if err != nil {
		// process err
		//panic(err)
		return nil
	}
	return result
}

func main() {
	target := CpuCurve{}
	target.addPoint("127.0.0.1", "*", 10)
	target.addPoint("127.0.0.1", "*", 20)
	target.addPoint("127.0.0.1", "*", 30)
	target.rangePoint("127.0.0.1", "1587889046161", "*")
}
