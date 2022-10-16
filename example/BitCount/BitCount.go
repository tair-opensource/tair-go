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

type BitCount struct {
	// field
}

func (l *BitCount) setBit(key string, offset int64, value int64) bool {
	_, err := tairClient.TrSetBit(ctx, key, offset, value).Result()
	if err != nil {
		// process err
		//panic(err)
		return false
	}
	return true

}
func (l *BitCount) bitCount(key string) int64 {
	result, err := tairClient.TrBitCount(ctx, key).Result()
	if err != nil {
		// process err
		//panic(err)
		return -1
	}
	return result
}

func main() {
	key := "BitCount"
	target := BitCount{}
	target.setBit(key, 0, 1)
	target.setBit(key, 1, 1)
	target.setBit(key, 2, 1)
	fmt.Println(target.bitCount(key))
}
