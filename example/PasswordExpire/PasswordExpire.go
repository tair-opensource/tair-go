package main

import (
	"context"
	"fmt"
	"time"

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

type PasswordExpire struct {
	// field
}

func (l *PasswordExpire) addUserPass(key string, user string, password string, timeout int64) bool {
	result, err := tairClient.ExHSetArgs(ctx, key, user, password, tair.ExHSetArgs{}.New().Ex(time.Duration(timeout))).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	return result == 1
}

func main() {
	key := "PasswordExpire"
	target := PasswordExpire{}
	target.addUserPass(key, "user1", "pd1", 5)
	target.addUserPass(key, "user2", "pd2", 10)
	time.Sleep(5 * time.Second)
	fmt.Println(tairClient.ExHGetAll(ctx, key))
}
