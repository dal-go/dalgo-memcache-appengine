package memcache4dalgo

import "context"

var Debugf func(ctx context.Context, format string, args ...any) = nil
var Warningf func(ctx context.Context, format string, args ...any) = nil
