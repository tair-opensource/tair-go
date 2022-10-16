package main

import (
	"context"
	"fmt"
	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
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

type CrawlerSystem struct {
}

func (l *CrawlerSystem) JudeUrlsExists(key string, urls ...string) []bool {
	result, err := tairClient.BfMExists(ctx, key, urls...).Result()
	if err != nil {
		// process err
		//panic(err)
		return nil
	}
	return result

}

func main() {
	key := "CrawlerSystem"
	target := CrawlerSystem{}
	tairClient.BfAdd(ctx, key, "abc")
	tairClient.BfAdd(ctx, key, "def")
	tairClient.BfAdd(ctx, key, "ghi")
	fmt.Println(target.JudeUrlsExists(key, "abc", "def", "xxx"))
}
