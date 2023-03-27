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

type BoundedCounter struct {
	// field
}

func (l *BoundedCounter) tryAcquire(key string, upperBoud int64, interval int64) bool {
	a := make([]string, 0)
	strings := append(a, key)
	_, err := tairClient.Eval(ctx, "if redis.call('exists', KEYS[1]) == 1 then return redis.call('EXINCRBY', KEYS[1], '1', 'MAX', ARGV[1], 'KEEPTTL')"+
		" else return redis.call('EXSET', KEYS[1], 0, 'EX', ARGV[2]) end", strings, upperBoud, interval).Result()
	if err != nil {
		// process err
		//panic(err)
		return true
	}
	return false
}

func main() {
	key := "rateLimiter"
	target := BoundedCounter{}
	size := 10
	for i := 0; i < size; i++ {
		fmt.Printf("attempt %v, result: %v\n", i, target.tryAcquire(key, 8, 10))
	}
}
