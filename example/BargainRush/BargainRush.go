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

type BargainRush struct {
	// field
}

func (l *BargainRush) bargainRush(key string, upperBound int64, lowerBound int64) bool {
	_, err := tairClient.ExIncrByArgs(ctx, key, -1, tair.ExIncrByArgs{}.New().Def(upperBound).Min(lowerBound)).Result()
	if err != nil {
		// process err
		//panic(err)
		return false
	}
	return true
}

func main() {
	key := "bargainRush"
	target := BargainRush{}
	size := 20
	for i := 0; i < size; i++ {
		fmt.Printf("attempt %v, result: %v\n", i, target.bargainRush(key, 10, 0))
	}
}
