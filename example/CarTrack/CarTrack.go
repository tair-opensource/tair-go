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

type CarTrack struct {
	// field
}

func (l *CarTrack) addCoordinate(key, ts string, longitude, latitude float64) bool {
	result, err := tairClient.GisAdd(ctx, key, ts, "POINT ("+strconv.FormatFloat(longitude, 'E', -1, 64)+" "+strconv.FormatFloat(latitude, 'E', -1, 64)+")").Result()
	if err != nil {
		// process err
		//panic(err)
	}
	if result == 1 {
		return true
	}
	return false
}

func (l *CarTrack) getAllCoordinate(key string) map[string]string {
	result, err := tairClient.GisGetAll(ctx, key).Result()
	if err != nil {
		// process err
		//panic(err)
		return nil
	}
	return result
}

func main() {
	key := "CarTrack"
	target := CarTrack{}
	target.addCoordinate(key, strconv.Itoa(int(time.Now().Unix())), 120.036188, 30.287922)
	time.Sleep(1 * time.Millisecond)
	target.addCoordinate(key, strconv.Itoa(int(time.Now().Unix())), 120.037625, 30.292225)
	time.Sleep(1 * time.Millisecond)
	target.addCoordinate(key, strconv.Itoa(int(time.Now().Unix())), 120.034435, 30.303303)
	fmt.Println(target.getAllCoordinate(key))
}
