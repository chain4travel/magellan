// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

import (
	"time"
)

type Transaction struct {
	ID      StringID `json:"id"`
	ChainID StringID `json:"chainID"`
	Type    string   `json:"type"`

	Inputs  []*Input  `json:"inputs,omitempty"`
	Outputs []*Output `json:"outputs,omitempty"`

	Memo []byte `json:"memo,omitempty"`

	InputTotals         AssetTokenCounts `json:"inputTotals,omitempty"`
	OutputTotals        AssetTokenCounts `json:"outputTotals,omitempty"`
	ReusedAddressTotals AssetTokenCounts `json:"reusedAddressTotals,omitempty"`

	CanonicalSerialization []byte    `json:"canonicalSerialization,omitempty"`
	CreatedAt              time.Time `json:"timestamp"`

	Txfee uint64 `json:"txFee"`

	Genesis bool `json:"genesis"`

	Rewarded     bool       `json:"rewarded,omitempty"`
	RewardedTime *time.Time `json:"rewardedTime,omitempty"`

	Epoch uint64 `json:"epoch,omitempty"`

	VertexID StringID `json:"vertexId,omitempty"`

	ValidatorNodeID StringID `json:"validatorNodeID,omitempty"`
	ValidatorStart  uint64   `json:"validatorStart,omitempty"`
	ValidatorEnd    uint64   `json:"validatorEnd,omitempty"`

	TxBlockID StringID `json:"txBlockId,omitempty"`

	Proposer *BlockProposal `json:"proposer,omitempty"`

	Score uint64 `json:"-"`
}

type Input struct {
	Output *Output            `json:"output"`
	Creds  []InputCredentials `json:"credentials"`
}

type Output struct {
	ID                     StringID    `json:"id"`
	TransactionID          StringID    `json:"transactionID"`
	OutputIndex            uint64      `json:"outputIndex"`
	AssetID                StringID    `json:"assetID"`
	Stake                  bool        `json:"stake"`
	Frozen                 bool        `json:"frozen"`
	Stakeableout           bool        `json:"stakeableout"`
	Genesisutxo            bool        `json:"genesisutxo"`
	OutputType             OutputType  `json:"outputType"`
	Amount                 TokenAmount `json:"amount"`
	Locktime               uint64      `json:"locktime"`
	StakeLocktime          uint64      `json:"stakeLocktime"`
	Threshold              uint64      `json:"threshold"`
	Addresses              []Address   `json:"addresses"`
	CAddresses             []string    `json:"caddresses"`
	CreatedAt              time.Time   `json:"timestamp"`
	RedeemingTransactionID StringID    `json:"redeemingTransactionID"`
	ChainID                StringID    `json:"chainID"`
	InChainID              StringID    `json:"inChainID"`
	OutChainID             StringID    `json:"outChainID"`
	GroupID                uint64      `json:"groupID"`
	Payload                []byte      `json:"payload"`
	Block                  string      `json:"block"`
	Nonce                  uint64      `json:"nonce"`
	RewardUtxo             bool        `json:"rewardUtxo"`
	Score                  uint64      `json:"-"`
}

type InputCredentials struct {
	Address   Address `json:"address"`
	PublicKey []byte  `json:"public_key"`
	Signature []byte  `json:"signature"`
}

type OutputAddress struct {
	OutputID  StringID `json:"output_id"`
	Address   Address  `json:"address"`
	Signature []byte   `json:"signature"`
	PublicKey []byte   `json:"-"`
}

type Asset struct {
	ID      StringID `json:"id"`
	ChainID StringID `json:"chainID"`

	Name          string      `json:"name"`
	Symbol        string      `json:"symbol"`
	Alias         string      `json:"alias"`
	CurrentSupply TokenAmount `json:"currentSupply"`
	CreatedAt     time.Time   `json:"timestamp"`

	Score uint64 `json:"-"`

	Denomination uint8 `json:"denomination"`
	VariableCap  uint8 `json:"variableCap"`
	Nft          uint8 `json:"nft"`
}

type AssetInfo struct {
	AssetID StringID `json:"id"`

	TransactionCount uint64      `json:"transactionCount"`
	UTXOCount        uint64      `json:"utxoCount"`
	Balance          TokenAmount `json:"balance"`
	TotalReceived    TokenAmount `json:"totalReceived"`
	TotalSent        TokenAmount `json:"totalSent"`
}

type AddressInfo struct {
	ChainID   StringID `json:"chainID"`
	Address   Address  `json:"address"`
	PublicKey []byte   `json:"publicKey"`

	Assets map[StringID]AssetInfo `json:"assets"`

	Score uint64 `json:"-"`
}

type AddressChainInfo struct {
	Address   Address   `json:"address"`
	ChainID   StringID  `json:"chainID"`
	CreatedAt time.Time `json:"timestamp"`
}

type OutputList struct {
	ListMetadata
	Outputs []*Output `json:"outputs"`
}

type CvmOutput struct {
	Type            CChainType  `json:"type"`
	TransactionType CChainType  `json:"transactionType"`
	Idx             uint64      `json:"idx"`
	Amount          TokenAmount `json:"amount"`
	Nonce           uint64      `json:"nonce"`
	ID              StringID    `json:"id"`
	TransactionID   StringID    `json:"transactionID"`
	Address         string      `json:"address"`
	AssetID         StringID    `json:"assetID"`
	CreatedAt       time.Time   `json:"timestamp"`
	ChainID         StringID    `json:"chainID"`
	Block           string      `json:"block"`
}

type ChainCounts struct {
	ChainID StringID `json:"chainID"`
	Total   string   `json:"total"`
}
