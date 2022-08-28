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
)
