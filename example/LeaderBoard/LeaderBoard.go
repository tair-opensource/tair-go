package main

import (
	"context"
	"fmt"
	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
	"strconv"
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

type LeaderBoard struct {
	// field
}

func (l *LeaderBoard) addUser(key, member string, scores ...string) bool {
	_, err := tairClient.ExZAddManyScore(ctx, key, member, scores...).Result()
	if err != nil {
		// process err
		//panic(err)
		return true
	}
	return false
}

func (l *LeaderBoard) top(key string, startOffSet int, endOffset int) []string {
	result, err := tairClient.ExZRevRange(ctx, key, startOffSet, endOffset).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	return result

}

func main() {
	key := "LeaderBoard"
	target := LeaderBoard{}
	// add three user
	target.addUser(key, "user1", strconv.Itoa(20), strconv.Itoa(10), strconv.Itoa(30))
	target.addUser(key, "user2", strconv.Itoa(20), strconv.Itoa(15), strconv.Itoa(10))
	target.addUser(key, "user3", strconv.Itoa(30), strconv.Itoa(10), strconv.Itoa(20))
	// get top 2
	fmt.Println(target.top(key, 0, 1))
}
