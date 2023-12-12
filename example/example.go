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
		Addr:     "xxx.redis.rds.aliyuncs.com:6379",
		Password: "xxx",
		DB:       0,
	})
}

func main() {
	err := tairClient.ExSet(ctx, "exkey", "exval").Err()
	if err != nil {
		fmt.Println(err.Error())
	}

	val, err := tairClient.ExGet(ctx, "exkey").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("get exkey values is: ", val)
}
