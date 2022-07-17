package tair_test

import (
	"context"
	"github.com/alibaba/tair-go/tair"
	"github.com/go-redis/redis/v8"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net"
	"time"
)

var (
	ctx      = context.Background()
	rdb      *redis.Client
	testHost = "192.168.220.137"
)

type clusterScenario struct {
	ports     []string
	nodeIDs   []string
	processes map[string]*redisProcess
	clients   map[string]*tair.TairClient
}

func (s *clusterScenario) addrs() []string {
	addrs := make([]string, len(s.ports))
	for i, port := range s.ports {
		addrs[i] = net.JoinHostPort(testHost, port)
	}
	return addrs
}

func (s *clusterScenario) newClusterClientUnstable(opt *redis.ClusterOptions) *tair.TairClusterClient {
	opt.Addrs = s.addrs()
	options := &tair.TairClusterOptions{ClusterOptions: opt}
	return tair.NewTairClusterClient(options)
}

func (s *clusterScenario) newClusterClient(
	ctx context.Context, opt *redis.ClusterOptions,
) *tair.TairClusterClient {
	client := s.newClusterClientUnstable(opt)

	err := eventually(func() error {
		if opt.ClusterSlots != nil {
			return nil
		}

		//state, err := client.LoadState(ctx)
		//if err != nil {
		//	return err
		//}
		//
		//if !state.IsConsistent(ctx) {
		//	return fmt.Errorf("cluster state is not consistent")
		//}

		return nil
	}, 30*time.Second)
	if err != nil {
		panic(err)
	}

	return client
}

var _ = Describe("TairClusterClient string", func() {
	//var failover bool
	var opt *redis.ClusterOptions
	var client *tair.TairClusterClient

	BeforeEach(func() {
		opt = redisClusterOptions()
		client = cluster.newClusterClient(ctx, opt)

		err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		_ = client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		})
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should GET/SET/DEL", func() {
		err := client.Get(ctx, "A").Err()
		Expect(err).To(Equal(redis.Nil))

		err = client.Set(ctx, "A", "VALUE", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() string {
			return client.Get(ctx, "A").Val()
		}, 30*time.Second).Should(Equal("VALUE"))

		cnt, err := client.Del(ctx, "A").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(cnt).To(Equal(int64(1)))
	})

	It("should ExSet ExGet", func() {
		result1, err1 := client.ExSetArgs(ctx, "foo", "bar", tair.ExSetArgs{}.New().Xx()).Result()
		Expect(err1).To(HaveOccurred())
		Expect(result1).To(Equal(""))

		result2, err2 := client.ExSetArgs(ctx, "foo", "bar", tair.ExSetArgs{}.New().Nx()).Result()
		Expect(err2).NotTo(HaveOccurred())
		Expect(result2).To(Equal("OK"))

		result3, err3 := client.ExSetArgs(ctx, "foo", "bar", tair.ExSetArgs{}.New().Abs(100)).Result()
		Expect(err3).NotTo(HaveOccurred())
		Expect(result3).To(Equal("OK"))

		result4, err4 := client.ExGet(ctx, "foo").Result()
		Expect(err4).NotTo(HaveOccurred())
		Expect(result4[0]).To(Equal("bar"))
		Expect(result4[1]).To(Equal(int64(100)))

		a := tair.ExSetArgs{}.New().Abs(88).Flags(99)
		exSetRes, err := client.ExSetArgs(ctx, "foo1", "bar1", a).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(exSetRes).To(Equal("OK"))

		res, err := client.ExGetWithFlags(ctx, "foo1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res[0]).To(Equal("bar1"))
		Expect(res[1]).To(Equal(int64(88)))
		Expect(res[2]).To(Equal(int64(99)))
	})

})

var _ = Describe("TairClusterClient zset", func() {
	//var failover bool
	var opt *redis.ClusterOptions
	var client *tair.TairClusterClient

	BeforeEach(func() {
		opt = redisClusterOptions()
		client = cluster.newClusterClient(ctx, opt)

		err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		_ = client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		})
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should Add", func() {
		res, err := client.ExZAdd(ctx, "k1", "90.1", "v1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(1)))

		zRangeRes, err := client.ExZRange(ctx, "k1", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(zRangeRes[0]).To(Equal("v1"))
	})

	It("exzadd test", func() {
		res, err := client.ExZAdd(ctx, "foo", "1", "a").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(1)))

		res, err = client.ExZAdd(ctx, "foo", "10", "b").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(1)))

		res, err = client.ExZAdd(ctx, "foo", "2", "a").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(0)))
	})

	It("exzadd params", func() {
		res, err := client.ExZAddArgs(ctx, "foo", "1", "a", tair.ExZAddArgs{}.New().Xx()).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(0)))

		res, err = client.ExZAdd(ctx, "foo", "1", "a").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(1)))

		res, err = client.ExZAddArgs(ctx, "foo", "2", "a", tair.ExZAddArgs{}.New().Nx()).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(0)))

		res, err = client.ExZAddManyMemberArgs(ctx, "foo", tair.ExZAddArgs{}.New().Ch(),
			tair.ExZAddMember{Score: "2", Member: "a"}, tair.ExZAddMember{Score: "1", Member: "b"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(2)))
	})

	It("exzrange basic", func() {
		res, err := client.ExZAddManyMember(ctx, "foo",
			tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
			tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(3)))

		ss, err := client.ExZRange(ctx, "foo", 0, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(ss).To(Equal([]string{"c", "a"}))

		ss, err = client.ExZRange(ctx, "foo", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(ss).To(Equal([]string{"c", "a", "b"}))

		ss, err = client.ExZRevRange(ctx, "foo", 0, 1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(ss).To(Equal([]string{"b", "a"}))
	})

})

var _ = Describe("TairClusterClient hash", func() {
	//var failover bool
	var opt *redis.ClusterOptions
	var client *tair.TairClusterClient

	BeforeEach(func() {
		opt = redisClusterOptions()
		client = cluster.newClusterClient(ctx, opt)

		err := client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		_ = client.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
			return master.FlushDB(ctx).Err()
		})
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("EXHGET", func() {
		res, err := client.ExHSet(ctx, "k1", "f1", "v1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(1)))
		res, err = client.Exists(ctx, "k1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(1)))
		result, err := client.ExHGet(ctx, "k1", "f1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal("v1"))
	})

	It("ExHSetByArgs", func() {
		a := tair.ExHSetArgs{}.New()
		a.Set = make(map[string]bool)
		a.Xx()
		res, err := client.ExHSetArgs(ctx, "k1", "f1", "v1", a).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(-1)))
		res, err = client.Exists(ctx, "k1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(0)))
	})

	It("ExHSetNx", func() {
		res, err := client.ExHSetNx(ctx, "k1", "f1", "v1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(1)))
		res, err = client.Exists(ctx, "k1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(int64(1)))
	})

})
