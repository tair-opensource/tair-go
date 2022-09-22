# tair-go

![build workflow](https://github.com/alibaba/tair-go/actions/workflows/go.yml/badge.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/alibaba/tair-go.svg)](https://pkg.go.dev/github.com/alibaba/tair-go)

English | [简体中文](./README-CN.md)

A client packaged based on [go-redis](https://github.com/go-redis/redis) that operates [Tair](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/apsaradb-for-redis-enhanced-edition-overview) For Redis Modules.

* [TairString](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairstring-commands), is a string that contains s version number.([Open sourced](https://github.com/alibaba/TairString))
* [TairHash](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairhash-commands), is a hash that allows you to specify the expiration time and verison number of a field.([Open sourced](https://github.com/alibaba/TairHash))
* [TairZset](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairzset-commands), allows you to sort data of the double type based on multiple dimensions. ([Open sourced](https://github.com/alibaba/TairZset))
* [TairBloom](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairbloom-commands), is a Bloom filter that supports dynamic scaling. (Coming soon)
* [TairRoaring](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairroaring-commands), is a more efficient and balanced type of compressed bitmaps recognized by the industry. (Coming soon)
* [TairSearch](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairsearch-command), is a full-text search module developed in-house based on Redis modules. (Coming soon)
* [TairDoc](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairdoc-commands), to perform create, read, update, and delete (CRUD) operations on JSON data. (Coming soon)
* [TairGis](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairgis-commands), allowing you to query points, linestrings, and polygons. (Coming soon)
* [TairTs](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/tairts-commands), is a time series data structure that is developed on top of Redis modules.  (Coming soon)
* [TairCpc](https://www.alibabacloud.com/help/en/apsaradb-for-redis/latest/taircpc-commands), is a data structure developed based on the compressed probability counting (CPC) sketch. (Coming soon)

## Installation

```
go get github.com/alibaba/tair-go
```

## Quickstart
An example of TairString is as follows:

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

## Tair All SDK

| language | GitHub |
|----------|---|
| Java     |https://github.com/alibaba/alibabacloud-tairjedis-sdk|
| Python   |https://github.com/alibaba/tair-py|
| Go       |https://github.com/alibaba/tair-go|
| .Net     |https://github.com/alibaba/AlibabaCloud.TairSDK|
