package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/alibaba/tair-go/tair"
	"github.com/redis/go-redis/v9"
	"math/rand"
)

var ctx = context.Background()

var tairClient *tair.TairClient // 全局客户端

var ip = "127.0.0.1"

func init() {
	tairClient = tair.NewTairClient(&redis.Options{
		Addr:     ip + ":" + "6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func ExampleTairVector() {
	index := "vector-index"
	dim := 4
	tairClient.TvsCreateIndex(ctx, index, dim, "HNSW", "L2",
		tair.TvsCreateIndexArgs{}.New().AutoGc(true).M(16).DataType("FLOAT16")).Result()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		fields := make(map[string]interface{})
		fields["field1"] = "value1"
		fields["field2"] = rand.Intn(100)
		fields["field3"] = rand.Float32()
		fields["field4"] = rand.Intn(2) == 0
		floats := make([]float32, dim)
		for i := 0; i < dim; i++ {
			floats[i] = rand.Float32()
		}
		b, _ := json.Marshal(floats)
		fields["VECTOR"] = string(b)

		result, err := tairClient.TvsHSet(ctx, index, key, tair.TvsHSetArgs{}.New().Fields(fields)).Result()
		if err != nil {
			fmt.Println(err.Error())
			panic(err)
		}

		if result != 5 {
			fmt.Println("tvsHSet failed")
			panic(result)
		}
	}

	result, err := tairClient.TvsKnnSearch(ctx, index, 10, "[0,0,0,0]", nil).Result()
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
	fmt.Println(result)

	tairClient.TvsDelIndex(ctx, index)
}

func main() {
	ExampleTairVector()
}
