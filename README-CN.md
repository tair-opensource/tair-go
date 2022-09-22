# tair-go

![build workflow](https://github.com/alibaba/tair-go/actions/workflows/go.yml/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/alibaba/tair-go.svg)](https://pkg.go.dev/github.com/alibaba/tair-go)

English | [简体中文](./README-CN.md)

基于 [go-redis](https://github.com/go-redis/redis) 封装，用于操作 [Tair Modules](https://help.aliyun.com/document_detail/145957.html) 的客户端。

- [TairHash](https://help.aliyun.com/document_detail/145970.html), 可实现 field 级别的过期。(已[开源](https://github.com/alibaba/TairHash))
- [TairString](https://help.aliyun.com/document_detail/145902.html), 支持 string 设置 version，增强的`cas`和`cad`命令可轻松实现分布式锁。(已[开源](https://github.com/alibaba/TairString))
- [TairZset](https://help.aliyun.com/document_detail/292812.html), 支持多维排序。(已[开源](https://github.com/alibaba/TairZset))
- [TairBloom](https://help.aliyun.com/document_detail/145972.html), 支持动态扩容的布隆过滤器。（待开源）
- [TairRoaring](https://help.aliyun.com/document_detail/311433.html), Roaring Bitmap, 使用少量的存储空间来实现海量数据的查询优化。（待开源）
- [TairSearch](https://help.aliyun.com/document_detail/417908.html), 支持ES-LIKE语法的全文索引和搜索模块。（待开源）
- [TairDoc](https://help.aliyun.com/document_detail/145940.html), 支持存储`JSON`类型。（待开源）
- [TairGis](https://help.aliyun.com/document_detail/145971.html), 支持地理位置点、线、面的相交、包含等关系判断。（待开源）
- [TairTs](https://help.aliyun.com/document_detail/408954.html), 时序数据结构，提供低时延、高并发的内存读写访问。（待开源）
- [TairCpc](https://help.aliyun.com/document_detail/410587.html), 基于CPC（Compressed Probability Counting）压缩算法开发的数据结构，支持仅占用很小的内存空间对采样数据进行高性能计算。（待开源）

## 安装
```
go get github.com/alibaba/tair-go
```

## 快速开始
一个 TairString 的示例如下所示:

go.mod
```
require (
	github.com/alibaba/tair-go v1.1.3
)
```

test.go
```Go
import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/alibaba/tair-go/tair"
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
