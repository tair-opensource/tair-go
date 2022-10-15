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

type JSONDocument struct {
	// field
}

func (l *JSONDocument) jsonSave(key, path, json string) bool {
	result, err := tairClient.JsonSet(ctx, key, path, json).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	if result == "OK" {
		return true
	}
	return false
}

func (l *JSONDocument) jsonGet(key, path string) string {
	result, err := tairClient.JsonGetPath(ctx, key, path).Result()
	if err != nil {
		// process err
		//panic(err)
	}
	return result
}

func main() {
	key := "JSONDocument"
	target := JSONDocument{}
	target.jsonSave(key, ".", "{\"name\":\"tom\",\"age\":22,\"description\":\"A man with a blue "+
		"lightsaber\",\"friends\":[]}")
	fmt.Println(target.jsonGet(key, ".description"))
}
