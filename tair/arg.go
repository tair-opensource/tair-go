package tair

type arger interface {
	GetArgs() []interface{}
}

type arg struct {
	arger
	Set map[string]bool
}

const (
	XX = "xx"
	NX = "nx"

	PX   = "px"
	EX   = "ex"
	EXAT = "exat"
	PXAT = "pxat"

	VER = "ver"
	ABS = "abs"

	MAX = "MAX"
	MIN = "MIN"

	DEF        = "def"
	NONEGATIVE = "nonegative"

	FLAGS   = "flags"
	KEEPTTL = "keepttl"

	CH         = "ch"
	INCR       = "incr"
	WITHSCORES = "withscores"
	LIMIT      = "limit"
	GT         = "gt"

	CAPACITY = "CAPACITY"
	ERROR    = "ERROR"
	NOCREATE = "NOCREATE"
	ITEMS    = "ITEMS"

	UNCOMPRESSED = "UNCOMPRESSED"
	DATA_ET      = "DATA_ET"
	CHUNK_SIZE   = "CHUNK_SIZE"
	LABELS       = "LABELS"

	MAXCOUNT    = "MAXCOUNT"
	WITHLABELS  = "WITHLABELS"
	REVERSE     = "REVERSE"
	FILTER      = "FILTER"
	AGGREGATION = "AGGREGATION"
	SUM         = "SUM"
	AVG         = "AVG"
	STDP        = "STD.P"
	STDS        = "STD.S"
	COUNT       = "COUNT"
	FIRST       = "FIRST"
	LAST        = "LAST"
	RANGE       = "RANGE"

	FORMAT   = "format"
	ROOTNAME = "rootname"
	ARRNAME  = "arrname"

	SIZE = "size"
	WIN  = "win"

	RADIUS = "radius"
	MEMBER = "member"

	WITHOUTWKT   = "withoutwkt"
	WITHVALUE    = "withvalue"
	WITHOUTVALUE = "withoutvalue"
	WITHDIST     = "withdist"
	ASC          = "asc"
	DESC         = "desc"
)

const (
	ValueIsNull      = "ERR:The value is null"
	ValueIsEmpty     = "ERR:The value is empty"
	KeyIsNull        = "ERR:The key is null"
	KeyIsEmpty       = "ERR:The key is empty"
	MultiExpireParam = "ERR:The expire param is not single"
	ExpIsSet         = "ERR:The expire param has been set"
	OptionIllegal    = "ERR:The option argument error"
)
