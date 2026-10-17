package dalgo2memcachegae

import (
	"google.golang.org/appengine/v2/memcache"
)

var (
	memcacheGet         = memcache.Get
	memcacheSet         = memcache.Set
	memcacheGetMulti    = memcache.GetMulti
	memcacheSetMulti    = memcache.SetMulti
	memcacheDelete      = memcache.Delete
	memcacheDeleteMulti = memcache.DeleteMulti
)
