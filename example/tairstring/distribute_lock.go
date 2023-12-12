package main

import (
	"context"
	"fmt"
	"github.com/alibaba/tair-go/tair"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"sync"
	"time"
)

var ctx = context.Background()

var tairClient *tair.TairClient

var lockKey = "LOCK_KEY"

func init() {
	tairClient = tair.NewTairClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})
}

// TryLock locks atomically via setnx
// requestId prevents the lock from being deleted by mistake
// expireTime is to prevent the deadlock of business machine downtime
func TryLock(key, requestId string, expireTime time.Duration) bool {
	res, err := tairClient.SetNX(ctx, key, requestId, expireTime).Result()
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	return res
}

// ReleaseLock atomically releases the lock via the CAD command
// requestId ensures that the released lock is added by itself
func ReleaseLock(key, requestId string) bool {
	res, err := tairClient.Cad(ctx, key, requestId).Result()
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	return res == int64(1)
}

type Account struct {
	Balance int
}

func randomString(length int) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	r.Read(b)
	return fmt.Sprintf("%x", b)[:length]
}

func depositAndWithdraw(account *Account) {
	var requestId = randomString(10)
	if TryLock(lockKey, requestId, 2*time.Second) {
		fmt.Println("Balance:", account.Balance)
		if account.Balance != 10 {
			panic(fmt.Sprintf("Balance should not be negative value: %d", account.Balance))
		}
		account.Balance += 1000
		time.Sleep(time.Second)
		account.Balance -= 1000
		ReleaseLock(lockKey, requestId)
	}
	time.Sleep(time.Second)
}

func main() {
	var wg sync.WaitGroup

	account := &Account{Balance: 10}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			for {
				depositAndWithdraw(account)
			}
		}()
	}
	wg.Wait()
}
