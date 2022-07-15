# tair-go

English | [简体中文](./README-CN.md)

基于 [go-redis](https://github.com/go-redis/redis) 封装，用于操作 [Tair Modules](https://help.aliyun.com/document_detail/145957.html) 的客户端。

- [TairHash](https://help.aliyun.com/document_detail/145970.html), 可实现 field 级别的过期。(已[开源](https://github.com/alibaba/TairHash))
- [TairString](https://help.aliyun.com/document_detail/145902.html), 支持 string 设置 version，增强的`cas`和`cad`命令可轻松实现分布式锁。(已[开源](https://github.com/alibaba/TairString))
- [TairZset](https://help.aliyun.com/document_detail/292812.html), 支持多维排序。(已[开源](https://github.com/alibaba/TairZset))

## 安装
```
go get github.com/alibaba/tair-go@v1.0.0
```

## 快速开始
一个 TairString 的示例如下所示:
```Go
import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"tair-go/tair"
)

var ctx = context.Background()

var tairClient *tair.TairClient

func init() {
	tairClient = tair.NewTairClient(&redis.Options{
		Addr:     "xxx.redis.rds.aliyuncs.com:6379",
		Password: "xxx",
		DB:       0,
	})
}

func main() {
	err := tairClient.ExSet(ctx, "exkey", "exval").Err()
	if err != nil {
		fmt.Println(err.Error())
	}

	val, err := tairClient.ExGet(ctx, "exkey").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("get exkey values is: ", val)
}
```
