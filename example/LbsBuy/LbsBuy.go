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

type LbsBuy struct {
	// field
}

func (l *LbsBuy) AddPolygon(key, storeName, storeWkt string) bool {
	result, err := tairClient.GisAdd(ctx, key, storeName, storeWkt).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	if result == 1 {
		return true
	}
	return false
}

func (l *LbsBuy) GetServiceStore(key, userLocation string) map[string]string {
	result, err := tairClient.GisContains(ctx, key, userLocation).Result()
	if err != nil {
		panic(err.Error())
	}
	return result

}

func main() {
	key := "LbsBuy"
	buy := LbsBuy{}
	buy.AddPolygon(key, "store-1", "POLYGON ((120.058897 30.283681, 120.093033 30.286363, 120.097632 30.269147, 120.050705 30.252863))")
	buy.AddPolygon(key, "store-2", "POLYGON ((120.026343 30.285739, 120.029289 30.280749, 120.0382 30.281997, 120.037051 30.288109))")

	fmt.Println(buy.GetServiceStore(key, "POINT(120.072264 30.27501)"))
}
