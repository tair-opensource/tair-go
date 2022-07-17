package main

import (
	"context"
	"fmt"
	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
	"reflect"
)

var ctx = context.Background()

var tairClient *tair.TairClient

var ip = "127.0.0.1"

func init() {
	tairClient = tair.NewTairClient(&redis.Options{
		Addr:     ip + ":" + "6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

//ExampleTairClusterClient test tair module command  with TairClusterClient
func ExampleTairClusterClient() {
	rdb := tair.NewTairClusterClient(&tair.TairClusterOptions{
		ClusterOptions: &redis.ClusterOptions{
			Addrs: []string{"", ip + ":30001", ip + ":30002",
				ip + ":30003", ip + ":30004", ip + ":30005", ip + ":30006"},
			// To route commands by latency or randomly, enable one of the following.
			//RouteByLatency: true,
			//RouteRandomly: true,
		},
	})
	setRes, err := rdb.ExSet(ctx, "key1", "value1").Result()
	if err != nil {
		fmt.Println("ExSet occurs err:", err)
	}
	fmt.Printf("ExSeT result: %v\n", setRes)

	getRes, err := rdb.ExGet(ctx, "key1").Result()
	if err != nil {
		fmt.Println("ExGet occurs err: ", err)
	}
	if ok := reflect.DeepEqual("value1", getRes[0]); !ok {
		fmt.Println("ExGet occurs err: ", err)
	} else {
		fmt.Printf("ExGeT result: %v\n", getRes)
	}
}

func main() {
	ExampleTairClusterClient()
}
