package main

import (
	"context"
	"fmt"
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

type FraudPrevention struct {
	// field
}

func (l *FraudPrevention) cpcAdd(key, item string) bool {
	result, err := tairClient.CpcUpdate(ctx, key, item).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	if "OK" == result {
		return true
	}
	return false

}
func (l *FraudPrevention) cpcEstimate(key string) float64 {
	result, err := tairClient.CpcEstimate(ctx, key).Result()
	if err != nil {
		// process err
		//panic(err)

	}
	return result
}

func main() {
	key := "FraudPrevention"
	target := FraudPrevention{}
	target.cpcAdd(key, "a")
	target.cpcAdd(key, "b")
	target.cpcAdd(key, "c")
	fmt.Println(target.cpcEstimate(key))
	target.cpcAdd(key, "d")
	fmt.Println(target.cpcEstimate(key))
}
