package memcache4dalgo

import (
	"context"
	"log"
)

var Debugf = func(ctx context.Context, format string, args ...any) {
	_ = ctx
	log.Printf(format, args...)
}

var Warningf = func(ctx context.Context, format string, args ...any) {
	Debugf(ctx, "WARNING: "+format, args...)
}
