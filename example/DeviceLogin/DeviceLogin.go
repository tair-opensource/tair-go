package main

import (
	"context"
	"fmt"
	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
	"strconv"
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

type DeviceLogin struct {
	// field
}

func (d *DeviceLogin) DeviceLogin(key, loginTime, device string, timeout int64) bool {
	result, err := tairClient.ExHSetArgs(ctx, key, loginTime, device, tair.ExHSetArgs{}.New().Ex(time.Duration(timeout))).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	if result == 1 {
		return true
	}
	return false
}

func main() {
	key := "DeviceLogin"
	login := DeviceLogin{}
	login.DeviceLogin(key, strconv.Itoa(int(time.Now().Unix())), "device1", 2)
	login.DeviceLogin(key, strconv.Itoa(int(time.Now().Unix())), "device2", 10)
	time.Sleep(5 * time.Second)
	fmt.Println(tairClient.ExHGetAll(ctx, key))

}
