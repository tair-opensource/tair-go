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

type GamePackage struct {
	// field
}

func (l *GamePackage) addEquipment(key, packagePath, equipment string) int64 {
	result, err := tairClient.JsonArrAppendWithPath(ctx, key, packagePath, equipment).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	return result
}

func main() {
	key := "GamePackage"
	target := GamePackage{}
	tairClient.JsonSet(ctx, key, ".", "[]")
	fmt.Println(target.addEquipment(key, ".", "\"lightsaber\""))
	fmt.Println(target.addEquipment(key, ".", "\"howitzer\""))
	fmt.Println(target.addEquipment(key, ".", "\"gun\""))

}
