package main

import (
	"context"
	"github.com/alibaba/tair-go/tair"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"time"
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

type BloomFilter struct {
	// field
}

func (l *BloomFilter) recommendedSystem(userId, docId string) {
	result, err := tairClient.BfExists(ctx, userId, docId).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	if result {
		// do nothing
	} else {
		// recommend to user sendRecommendMsg(docid);
		// add userid with docid
		tairClient.BfAdd(ctx, userId, docId)
	}
}

func randStr(size int) string {
	str := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := []byte(str)
	var result []byte
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(100000))))
	for i := 0; i < size; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}
func main() {
	key := "BloomFilter"
	target := BloomFilter{}
	target.recommendedSystem(key, randStr(10))
	target.recommendedSystem(key, randStr(10))
	target.recommendedSystem(key, randStr(10))
	target.recommendedSystem(key, randStr(10))

}
