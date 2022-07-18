package tair_test

import (
	"context"
	"sort"
	"time"

	"github.com/alibaba/tair-go/tair"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sort"
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
	Describe("tair hash", func() {
		It("EXHGET", func() {
			res, err := tairClient.ExHSet(ctx, "k1", "f1", "v1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))
			res, err = tairClient.Exists(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))
			result, err := tairClient.ExHGet(ctx, "k1", "f1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal("v1"))
		})

		It("ExHSetByArgs", func() {
			a := tair.ExHSetArgs{}.New()
			a.Set = make(map[string]bool)
			a.Xx()
			res, err := tairClient.ExHSetArgs(ctx, "k1", "f1", "v1", a).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(-1)))
			res, err = tairClient.Exists(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))
		})

		It("ExHSetNx", func() {
			res, err := tairClient.ExHSetNx(ctx, "k1", "f1", "v1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))
			res, err = tairClient.Exists(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))
		})

		It("EXHMGET", func() {
			a := make(map[string]string)
			a["f1"] = "v1"
			a["f2"] = "v2"
			a["f3"] = "v3"
			res2, err2 := tairClient.ExHMSet(ctx, "k1", a).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res2).To(Equal("OK"))
			result, err := tairClient.ExHMGet(ctx, "k1", "f1", "f2", "f3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(result[0]).To(Equal("v1"))
			Expect(result[1]).To(Equal("v2"))
			Expect(result[2]).To(Equal("v3"))
		})

		It("ExHMSetWithOpts", func() {
			b := tair.ExHMSetWithOptsArgs{}.New()
			b.Field("f1")
			b.Value("v1")
			b.SetExp(5)
			b.SetVer(99)
			_, err := tairClient.ExHMSetWithOpts(ctx, "k1", b).Result()
			Expect(err).NotTo(HaveOccurred())
		})

		It("ExHPExpire", func() {
			res, err := tairClient.ExHSet(ctx, "k1", "f1", "v1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))
			tairClient.ExHExpire(ctx, "k1", "f1", 1)
			time.Sleep(time.Duration(2) * time.Second)
			res, err1 := tairClient.Exists(ctx, "k1").Result()
			Expect(err1).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))
		})

		It("ExHSetVer", func() {
			res, err := tairClient.ExHSet(ctx, "k1", "f1", "v1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))

			res1, err := tairClient.ExHSetVer(ctx, "k1", "f1", 10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res1).To(Equal(true))

			res, err = tairClient.ExHVer(ctx, "k1", "f1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(10)))
		})

		It("ExHIncrBy", func() {
			res, err := tairClient.ExHIncrBy(ctx, "k1", "f1", 1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))

			res, err = tairClient.ExHIncrBy(ctx, "k1", "f1", -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))

			res, err = tairClient.ExHIncrBy(ctx, "k1", "f1", -10).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(-10)))
		})

		It("ExHIncrByArgs", func() {
			_, err := tairClient.ExHIncrByArgs(ctx, "k1", "f1", 11, tair.ExHIncrArgs{}.New().Min(0).Max(10)).Result()
			Expect(err.Error()).To(ContainSubstring("ERR increment or decrement would overflow"))
		})

		It("ExHIncrByFloat", func() {
			res, err := tairClient.ExHIncrByFloat(ctx, "k1", "f1", 1.5).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("1.5"))

			res, err = tairClient.ExHIncrByFloat(ctx, "k1", "f1", -1.5).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("0"))

			res, err = tairClient.ExHIncrByFloat(ctx, "k1", "f1", -10.7).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("-10.7"))
		})

		It("ExHIncrByFloat expire", func() {
			res, err := tairClient.ExHIncrByFloatArgs(ctx, "k1", "f1", 5.1, tair.ExHIncrArgs{}.New().Ex(1)).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("5.1"))

			time.Sleep(time.Duration(2) * time.Second)

			res2, err := tairClient.ExHLen(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res2).To(Equal(int64(0)))
		})

		It("ExHGetWithVer", func() {
			res, err := tairClient.ExHSet(ctx, "k1", "f1", "v1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))

			res2, err := tairClient.ExHGetWithVer(ctx, "k1", "f1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res2[0]).To(Equal("v1"))
			Expect(res2[1]).To(Equal(int64(1)))
		})

		It("ExHMGetWithVer", func() {
			a := make(map[string]string)
			a["f1"] = "v1"
			a["f2"] = "v2"
			a["f3"] = "v3"
			res2, err2 := tairClient.ExHMSet(ctx, "k1", a).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res2).To(Equal("OK"))

			result, err := tairClient.ExHMGetWithVer(ctx, "k1", "f1", "f2", "f3").Result()
			Expect(err).NotTo(HaveOccurred())
			v1 := make([]interface{}, 0)
			v1 = append(v1, "v1", int64(1))
			Expect(result[0]).To(Equal(v1))
		})

		It("ExHDel ExHLen ExHExists ExHStrLen", func() {
			a := make(map[string]string)
			a["f1"] = "v1"
			a["f2"] = "v2"
			a["f3"] = "v3"
			res2, err2 := tairClient.ExHMSet(ctx, "k1", a).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res2).To(Equal("OK"))

			res, err := tairClient.ExHDel(ctx, "k1", "not-exists").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))

			res, err = tairClient.ExHLen(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(3)))

			res, err = tairClient.ExHDel(ctx, "k1", "f1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(1)))

			res3, err := tairClient.ExHExists(ctx, "k1", "f1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res3).To(Equal(false))

			res, err = tairClient.ExHLen(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(2)))

			res, err = tairClient.ExHStrLen(ctx, "k1", "f2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(2)))
		})

		It("ExHKeys ExHVals", func() {
			a := make(map[string]string)
			a["f1"] = "v1"
			a["f2"] = "v2"
			a["f3"] = "v3"
			res2, err2 := tairClient.ExHMSet(ctx, "k1", a).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res2).To(Equal("OK"))

			res, err := tairClient.ExHKeys(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			sort.Strings(res)
			Expect(res).To(Equal([]string{"f1", "f2", "f3"}))

			res, err = tairClient.ExHVals(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			sort.Strings(res)
			Expect(res).To(Equal([]string{"v1", "v2", "v3"}))
		})

		It("ExHGetAll", func() {
			a := make(map[string]string)
			a["f1"] = "v1"
			a["f2"] = "v2"
			a["f3"] = "v3"
			res2, err2 := tairClient.ExHMSet(ctx, "k1", a).Result()
			Expect(err2).NotTo(HaveOccurred())
			Expect(res2).To(Equal("OK"))

			res, err := tairClient.ExHGetAll(ctx, "k1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"}))
		})
	})
})
