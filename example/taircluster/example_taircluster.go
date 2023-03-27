package main

import (
	"context"
	"fmt"
	"reflect"

	"github.com/alibaba/tair-go/tair"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

var clusterClient *tair.TairClusterClient

var ip = "127.0.0.1"

func init() {
	clusterClient = tair.NewTairClusterClient(&tair.TairClusterOptions{
		ClusterOptions: &redis.ClusterOptions{
			Addrs: []string{
				"", ip + ":30001", ip + ":30002",
				ip + ":30003", ip + ":30004", ip + ":30005", ip + ":30006",
			},
		},
	})
}

// ExampleTairClusterClient test tair module command  with TairClusterClient
func ExampleTairClusterClient() {
	setRes, err := clusterClient.ExSet(ctx, "key1", "value1").Result()
	if err != nil {
		fmt.Println("ExSet occurs err:", err)
	}
	fmt.Printf("ExSeT result: %v\n", setRes)

	getRes, err := clusterClient.ExGet(ctx, "key1").Result()
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
