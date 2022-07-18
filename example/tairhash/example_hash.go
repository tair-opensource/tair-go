package main

import (
	"context"
	"fmt"

	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()

var tairClient *tair.TairClient // 全局客户端

var ip = "127.0.0.1"

func init() {
	tairClient = tair.NewTairClient(&redis.Options{
		Addr:     ip + ":" + "6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func ExampleTairHash() {
	err := tairClient.ExHSet(ctx, "h-k-1", "f-1", "v-1").Err()
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	err = tairClient.ExHSet(ctx, "h-k-1", "f-2", "v-2").Err()
	if err != nil {
		fmt.Println(err.Error())
	}
	val, err := tairClient.ExHGet(ctx, "h-k-1", "f-1").Result()
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	fmt.Println("key", val)
	val, err = tairClient.ExHGet(ctx, "h-k-1", "f-2").Result()
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	fmt.Println("key", val)
}

func main() {
	ExampleTairHash()
}
