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

func main() {
	key := "MultiIndexSearch"
	tairClient.TftCreateIndex(ctx, key, "{\"mappings\":{\"properties\":{\"departure\":{\"type\":\"keyword\"},"+
		"\"destination\":{\"type\":\"keyword\"},\"date\":{\"type\":\"keyword\"},\"seat\":{\"type\":\"keyword\"},"+
		"\"with\":{\"type\":\"keyword\"},\"flight_id\":{\"type\":\"keyword\"},\"price\":{\"type\":\"double\"},"+
		"\"departure_time\":{\"type\":\"long\"},\"destination_time\":{\"type\":\"long\"}}}}")

	tairClient.TftAddDoc(ctx, key, "{\"departure\":\"zhuhai\",\"destination\":\"hangzhou\",\"date\":\"2022-09-01\","+
		"\"seat\":\"first\",\"with\":\"baby\",\"flight_id\":\"CZ1000\",\"price\":986.1,"+
		"\"departure_time\":1661991010,\"destination_time\":1661998210}")

	request := "{\"sort\":[\"departure_time\"],\"query\":{\"bool\":{\"must\":[{\"term\":{\"date\":\"2022-09" +
		"-01\"}},{\"term\":{\"seat\":\"first\"}}]}}}"

	fmt.Println(tairClient.TftSearch(ctx, key, request))

}
