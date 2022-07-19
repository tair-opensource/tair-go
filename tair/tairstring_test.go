package tair_test

import (
	"context"

	"github.com/alibaba/tair-go/tair"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("tair string commands", func() {
	ctx := context.TODO()
	var tairClient *tair.TairClient
	BeforeEach(func() {
		tairClient = tair.NewTairClient(redisOptions())
		Expect(tairClient.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(tairClient.Close()).NotTo(HaveOccurred())
	})

	Describe("tair string", func() {
		It("Cas", func() {
			tairClient.Set(ctx, "k1", "v1", 0)
			n, err := tairClient.Cas(ctx, "k1", "v2", "v3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
			n, err = tairClient.Cas(ctx, "k1", "v1", "v3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))
			res, err := tairClient.Get(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("v3"))
		})
		It("Cas Args", func() {
			tairClient.Set(ctx, "foo", "bzz", 0)
			tairClient.CasArgs(ctx, "foo", "bzz", "too", tair.CasArgs{}.New().Ex(1))
			result, err := tairClient.Get(ctx, "foo").Result()
			Expect(result).To(Equal("too"))
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(time.Duration(2) * time.Second)
			result1, err1 := tairClient.Get(ctx, "foo").Result()
			Expect(result1).To(Equal(""))
			Expect(err1).To(HaveOccurred())
		})
		It("Cad", func() {
			tairClient.Set(ctx, "foo", "bar", 0)
			res, err := tairClient.Cad(ctx, "foo", "bzz").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))
			res1, err1 := tairClient.Cad(ctx, "foo", "bar").Result()
			Expect(err1).NotTo(HaveOccurred())
			Expect(res1).To(Equal(int64(1)))
		})
		It("ExSetArgs", func() {
			result2, err2 := tairClient.ExSetArgs(ctx, "foo", "bar", tair.ExSetArgs{}.New().Xx()).Result()
			Expect(err2).To(HaveOccurred())
			Expect(result2).To(Equal(""))
			result3, err3 := tairClient.ExSetArgs(ctx, "foo", "bar", tair.ExSetArgs{}.New().Nx()).Result()
			Expect(err3).NotTo(HaveOccurred())
			Expect(result3).To(Equal("OK"))
		})
		It("ExGet", func() {
			result2, err2 := tairClient.ExSetArgs(ctx, "foo", "bar", tair.ExSetArgs{}.New().Abs(100)).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(result2).To(Equal("OK"))
			result4, err4 := tairClient.ExGet(ctx, "foo").Result()
			Expect(err4).NotTo(HaveOccurred())
			Expect(result4[0]).To(Equal("bar"))
			Expect(result4[1]).To(Equal(int64(100)))
		})
		It("ExGet with flags", func() {
			a := tair.ExSetArgs{}.New()
			a.Abs(88)
			a.Flags(99)
			exSetRes, err := tairClient.ExSetArgs(ctx, "k", "v", a).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(exSetRes).To(Equal("OK"))
			res, err := tairClient.ExGetWithFlags(ctx, "k").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res[0]).To(Equal("v"))
			Expect(res[1]).To(Equal(int64(88)))
			Expect(res[2]).To(Equal(int64(99)))
		})
		It("ExIncrBy", func() {
			result, err := tairClient.ExIncrBy(ctx, "foo", 100).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(int64(100)))
			a := tair.ExIncrByArgs{}.New()
			a.Max(150)
			_, err1 := tairClient.ExIncrByArgs(ctx, "foo", 100, a).Result()
			Expect(err1).To(HaveOccurred())
		})
		It("ExIncrByArgs", func() {
			result, err := tairClient.ExIncrBy(ctx, "foo", 100).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(int64(100)))
			a := tair.ExIncrByArgs{}.New()
			a.Max(300)
			res1, err1 := tairClient.ExIncrByArgs(ctx, "foo", 100, a).Result()
			Expect(err1).NotTo(HaveOccurred())
			Expect(res1).To(Equal(int64(200)))
		})
		It("ExIncrByFloat", func() {
			tairClient.ExSet(ctx, "foo", 100)
			result, err := tairClient.ExIncrByFloat(ctx, "foo", 10.123).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(110.123))
		})
		It("ExCas", func() {
			tairClient.ExSet(ctx, "foo", "bar")
			res2, err2 := tairClient.ExCas(ctx, "foo", "bzz", 1).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res2[0]).To(Equal("OK"))
			Expect(res2[1]).To(Equal(""))
			Expect(res2[2]).To(Equal(int64(2)))
			res3, err3 := tairClient.ExCas(ctx, "foo", "bee", 1).Result()
			Expect(err3).NotTo(HaveOccurred())
			Expect(res3[0]).To(Equal("CAS_FAILED"))
			Expect(res3[1]).To(Equal("bzz"))
			Expect(res3[2]).To(Equal(int64(2)))
		})

		It("ExCad", func() {
			tairClient.ExSet(ctx, "foo", "bar")
			result, err := tairClient.ExCad(ctx, "foo", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(int64(0)))
			result1, err1 := tairClient.ExCad(ctx, "foo", 1).Result()
			Expect(err1).NotTo(HaveOccurred())
			Expect(result1).To(Equal(int64(1)))
		})
		It("EXAPPEND", func() {
			result, err := tairClient.ExAppend(ctx, "exstringkey ", "foo", "nx", "ver", 99).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(int64(1)))
		})
		It("EXPREPEND", func() {
			result, err := tairClient.ExPreAppend(ctx, "exstringkey ", "foo", "nx", "ver", 99).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(int64(1)))
		})
		It("EXGAE", func() {
			a := tair.ExSetArgs{}.New()
			a.Ex(10)
			a.Flags(123)
			tairClient.ExSetArgs(ctx, "exstringkey", "foo", a)
			tairClient.TTL(ctx, "exstringkey")
			result, err := tairClient.ExGae(ctx, "exstringkey", "ex", 20).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result[0]).To(Equal("foo"))
			Expect(result[1]).To(Equal(int64(1)))
			Expect(result[2]).To(Equal(int64(123)))
		})
	})

})
