// Copyright (C) 2022, Chain4Travel AG. All rights reserved.
//
// This file is a derived work, based on ava-labs code whose
// original notices appear below.
//
// It is distributed under the same license conditions as the
// original code from which it is derived.
//
// Much love to the original authors for their work.
// **********************************************************
// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/chain4travel/magellan/caching"
	"github.com/chain4travel/magellan/cfg"
	"github.com/chain4travel/magellan/services/indexes/avax"
	"github.com/chain4travel/magellan/services/indexes/params"
	"github.com/chain4travel/magellan/servicesctrl"
	"github.com/chain4travel/magellan/utils"
	"github.com/gocraft/web"
)

// ErrCacheableFnFailed is returned when the execution of a CacheableFn
// fails.
var ErrCacheableFnFailed = errors.New("failed to load resource")

// Context is the base context for APIs in the magellan systems
type Context struct {
	sc *servicesctrl.Control

	networkID   uint32
	avaxAssetID ids.ID

	delayCache  *caching.DelayCache
	avaxReader  *avax.Reader
	connections *utils.Connections
}

// NetworkID returns the networkID this request is for
func (c *Context) NetworkID() uint32 {
	return c.networkID
}

func (c *Context) cacheGet(key string) ([]byte, error) {
	ctxget, cancelFnGet := context.WithTimeout(context.Background(), cfg.CacheTimeout)
	defer cancelFnGet()
	// Get from cache or, if there is a cache miss, from the cacheablefn
	return c.delayCache.Cache.Get(ctxget, key)
}

func (c *Context) cacheRun(reqTime time.Duration, cacheable caching.Cacheable) (interface{}, error) {
	ctxreq, cancelFnReq := context.WithTimeout(context.Background(), reqTime)
	defer cancelFnReq()

	return cacheable.CacheableFn(ctxreq)
}

// WriteCacheable writes to the http response the output of the given Cacheable's
// function, either from the cache or from a new execution of the function
func (c *Context) WriteCacheable(w http.ResponseWriter, cacheable caching.Cacheable) {
	key := caching.CacheKey(c.NetworkID(), cacheable.Key...)

	// Get from cache or, if there is a cache miss, from the cacheablefn
	resp, err := c.cacheGet(key)
	switch err {
	case nil:
	default:
		var obj interface{}
		obj, err = c.cacheRun(cfg.RequestTimeout, cacheable)
		if err == nil {
			resp, err = json.Marshal(obj)
			if err == nil {
				c.delayCache.Worker.TryEnque(&caching.CacheJob{Key: key, Body: &resp, TTL: cacheable.TTL})
			}
		}
	}

	// Write error or response
	if err != nil {
		c.sc.Log.Warn("server error",
			zap.Error(err),
		)
		c.WriteErr(w, 500, ErrCacheableFnFailed)
		return
	}
	WriteJSON(w, resp)
}

// WriteErr writes an error response to the http response
func (c *Context) WriteErr(w http.ResponseWriter, code int, err error) {
	errBytes, err := json.Marshal(&ErrorResponse{
		Code:    code,
		Message: err.Error(),
	})
	if err != nil {
		w.WriteHeader(500)
		c.sc.Log.Warn("marshal error",
			zap.Error(err),
		)
		return
	}

	w.WriteHeader(code)
	fmt.Fprint(w, string(errBytes))
}

func (*Context) setHeaders(w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
	h := w.Header()
	h.Add("access-control-allow-headers", "Accept, Content-Type, Content-Length, Accept-Encoding")
	h.Add("access-control-allow-methods", "GET")
	h.Add("access-control-allow-origin", "*")

	h.Add("Content-Type", "application/json")

	next(w, r)
}

func (*Context) notFoundHandler(w web.ResponseWriter, r *web.Request) {
	WriteErr(w, 404, "Not Found")
}

func (c *Context) cacheKeyForID(name string, id string) []string {
	return []string{"avax", name, params.CacheKey("id", id)}
}

func (c *Context) cacheKeyForParams(name string, p params.Param) []string {
	return append([]string{"avax", name}, p.CacheKey()...)
}

func newContextSetter(sc *servicesctrl.Control, networkID uint32, connections *utils.Connections, delayCache *caching.DelayCache) func(*Context, web.ResponseWriter, *web.Request, web.NextMiddlewareFunc) {
	return func(c *Context, w web.ResponseWriter, r *web.Request, next web.NextMiddlewareFunc) {
		c.sc = sc
		c.connections = connections
		c.delayCache = delayCache
		c.networkID = networkID

		// Execute handler
		next(w, r)
	}
}
