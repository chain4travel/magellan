// Copyright (C) 2022, Chain4Travel AG. All rights reserved.
//
// This file is a derived work, based on ava-labs code.
//
// It is distributed under the same license conditions as the
// original code from which it is derived.
//
// Much love to the original authors for their work.
// **********************************************************

package modelsc

import (
	"context"
	"sync"
	"time"

	"github.com/chain4travel/caminoethvm/core/types"
	"github.com/chain4travel/caminoethvm/rpc"
)

type Client struct {
	rpcClient *rpc.Client
	// ethClient ethclient.Client
	lock sync.Mutex
}

func NewClient(url string) (*Client, error) {
	rc, err := rpc.Dial(url)
	if err != nil {
		return nil, err
	}
	cl := &Client{}
	cl.rpcClient = rc
	// cl.ethClient = ethclient.NewClient(rc)
	return cl, nil
}

func (c *Client) Close() {
	c.rpcClient.Close()
}

func (c *Client) ReadReceipt(txHash string, rpcTimeout time.Duration) (*types.Receipt, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	ctx, cancelCTX := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancelCTX()

	result := &types.Receipt{}
	if err := c.rpcClient.CallContext(ctx, result, "eth_getTransactionReceipt", txHash); err != nil {
		return nil, err
	}

	return result, nil
}
