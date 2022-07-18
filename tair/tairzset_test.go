package tair_test

import (
	"context"
	"github.com/alibaba/tair-go/tair"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("tair zset commands", func() {
	ctx := context.TODO()
	var tairClient *tair.TairClient
	BeforeEach(func() {
		tairClient = tair.NewTairClient(redisOptions())
		Expect(tairClient.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(tairClient.Close()).NotTo(HaveOccurred())
	})

	Describe("tair zset", func() {
		It("zset should Add", func() {
			res, err := tairClient.ExZAdd(ctx, "k1", "90.1", "v1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))
			zRangeRes, err := tairClient.ExZRange(ctx, "k1", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(zRangeRes[0]).To(Equal("v1"))
		})

		It("exzadd test", func() {
			res, err := tairClient.ExZAdd(ctx, "foo", "1", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))

			res, err = tairClient.ExZAdd(ctx, "foo", "10", "b").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))

			res, err = tairClient.ExZAdd(ctx, "foo", "2", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))
		})

		It("exzadd params", func() {
			res, err := tairClient.ExZAddArgs(ctx, "foo", "1", "a", tair.ExZAddArgs{}.New().Xx()).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))

			res, err = tairClient.ExZAdd(ctx, "foo", "1", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))

			res, err = tairClient.ExZAddArgs(ctx, "foo", "2", "a", tair.ExZAddArgs{}.New().Nx()).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))

			res, err = tairClient.ExZAddManyMemberArgs(ctx, "foo", tair.ExZAddArgs{}.New().Ch(),
				tair.ExZAddMember{Score: "2", Member: "a"}, tair.ExZAddMember{Score: "1", Member: "b"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(2)))
		})

		It("exzrange basic", func() {
			res, err := tairClient.ExZAddManyMember(ctx, "foo",
				tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
				tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(3)))

			ss, err := tairClient.ExZRange(ctx, "foo", 0, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ss).To(Equal([]string{"c", "a"}))

			ss, err = tairClient.ExZRange(ctx, "foo", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ss).To(Equal([]string{"c", "a", "b"}))

			ss, err = tairClient.ExZRevRange(ctx, "foo", 0, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ss).To(Equal([]string{"b", "a"}))
		})

		It("exzrange by lex", func() {
			res, err := tairClient.ExZAddManyMember(ctx, "foo",
				tair.ExZAddMember{Score: "1", Member: "aa"}, tair.ExZAddMember{Score: "1", Member: "c"},
				tair.ExZAddMember{Score: "1", Member: "bb"}, tair.ExZAddMember{Score: "1", Member: "d"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(4)))

			ss, err := tairClient.ExZRangeByLex(ctx, "foo", "(aa", "[c").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ss).To(Equal([]string{"bb", "c"}))

			ss, err = tairClient.ExZRangeByLexWithArgs(ctx, "foo", "-", "+", tair.ExZRangeArgs{}.New().Limit(1, 2)).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ss).To(Equal([]string{"bb", "c"}))
		})

		It("exzrem basic", func() {
			res, err := tairClient.ExZAddManyMember(ctx, "foo",
				tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "2", Member: "b"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(2)))

			is, err := tairClient.ExZRem(ctx, "foo", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(is).To(Equal(int64(1)))
		})

		It("exzincrby bacis", func() {
			res, err := tairClient.ExZAddManyMember(ctx, "foo",
				tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "2", Member: "b"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(2)))

			ss, err := tairClient.ExZIncrBy(ctx, "foo", "2", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ss).To(Equal("3"))
		})

		It("exzrank basic", func() {
			res, err := tairClient.ExZAddManyMember(ctx, "foo",
				tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "2", Member: "b"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(2)))

			res, err = tairClient.ExZRank(ctx, "foo", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))

			res, err = tairClient.ExZRank(ctx, "foo", "b").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))

			res, err = tairClient.ExZRevRank(ctx, "foo", "a").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))
		})

		It("exzrangewithscore basic", func() {
			res, err := tairClient.ExZAddManyMember(ctx, "foo",
				tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
				tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(3)))

			ss, err := tairClient.ExZRangeWithScores(ctx, "foo", 0, 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ss).To(Equal([]string{"c", "0.10000000000000001", "a", "2"}))
		})

		It("exzcard basic", func() {
			res, err := tairClient.ExZAddManyMember(ctx, "foo",
				tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
				tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(3)))

			res, err = tairClient.ExZCard(ctx, "foo").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(3)))

			ss, err := tairClient.ExZScore(ctx, "foo", "b").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ss).To(Equal("10"))
		})

		It("exzcount basic", func() {
			res, err := tairClient.ExZAddManyMember(ctx, "foo",
				tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
				tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(3)))

			res, err = tairClient.ExZCount(ctx, "foo", "0.01", "2.1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(2)))
		})

		It("exzremrange by rank", func() {
			res, err := tairClient.ExZAddManyMember(ctx, "foo",
				tair.ExZAddMember{Score: "1", Member: "a"}, tair.ExZAddMember{Score: "10", Member: "b"},
				tair.ExZAddMember{Score: "0.1", Member: "c"}, tair.ExZAddMember{Score: "2", Member: "a"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(3)))

			res, err = tairClient.ExZRemRangeByRank(ctx, "foo", 0, 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))

			ss, err := tairClient.ExZRange(ctx, "foo", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ss).To(Equal([]string{"a", "b"}))
		})
	})
})
