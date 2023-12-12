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

type FullTextSearch struct {
	// field
}

func (l *FullTextSearch) createIndex(index, schema string) bool {
	_, err := tairClient.TftCreateIndex(ctx, index, schema).Result()
	if err != nil {
		// process err
		//panic(err)
		return true
	}
	return false

}

func (l *FullTextSearch) addDoc(index, doc string) string {
	result, err := tairClient.TftAddDoc(ctx, index, doc).Result()
	if err != nil {
		// process err
		//panic(err)
		return ""
	}
	return result
}

func (l *FullTextSearch) searchIndex(index, request string) string {
	result, err := tairClient.TftSearch(ctx, index, request).Result()
	if err != nil {
		// process err
		//panic(err)
		return ""
	}
	return result
}

func main() {
	key := "FullTextSearch"
	target := FullTextSearch{}

	target.createIndex(key, "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"keyword\"},\"content\":{\"type\":\"text\",\"analyzer\":\"jieba\"},\"time\":{\"type\":\"long\"},\"author\":{\"type\":\"keyword\"},\"heat\":{\"type\":\"integer\"}}}}")
	target.addDoc(key, "{\"title\":\"Does not work\",\"content\":\"It was removed from the beta a while ago. You should have expected it was going to be removed from the stable client as well at some point.\",\"time\":1541713787,\"author\":\"cSg|mc\",\"heat\":10}")
	target.addDoc(key, "{\"title\":\"paypal no longer launches to purchase\",\"content\":\"Since the last update, I cannot purchase anything via the app. I just keep getting a screen that says\",\"time\":1551476987,\"author\":\"disasterpeac\",\"heat\":2}")
	target.addDoc(key, "{\"title\":\"cat not login\",\"content\":\"Hey! I am trying to login to steam beta client via qr code / steam guard code but both methods does not work for me\",\"time\":1664488187,\"author\":\"7xx\",\"heat\":100}")

	request := "{\"sort\":[{\"heat\":{\"order\":\"desc\"}}],\"query\":{\"match\":{\"content\":\"paypal work code\"}}}"
	fmt.Println(target.searchIndex(key, request))
}
