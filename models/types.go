// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package models

type CChainType uint16

var (
	AVMName = "avm"
	PVMName = "pvm"
	CVMName = "cvm"

	CChainIn     CChainType = 1
	CchainOut    CChainType = 2
	CChainImport CChainType = 1
	CChainExport CChainType = 2

	OutputTypesSECP2556K1Transfer OutputType = 7
	OutputTypesSECP2556K1Mint     OutputType = 6
	OutputTypesNFTMint            OutputType = 10
	OutputTypesNFTTransfer        OutputType = 11
	OutputTypesAtomicExportTx     OutputType = 0xFFFFFFF1
	OutputTypesAtomicImportTx     OutputType = 0xFFFFFFF2

	BlockTypeProposal BlockType = 0x0
	BlockTypeAbort    BlockType = 0x1
	BlockTypeCommit   BlockType = 0x2
	BlockTypeStandard BlockType = 0x3
	BlockTypeAtomic   BlockType = 0x4

	TransactionTypeBase                       TransactionType = 0x0
	TransactionTypeCreateAsset                TransactionType = 0x1
	TransactionTypeOperation                  TransactionType = 0x2
	TransactionTypeAVMImport                  TransactionType = 0x3
	TransactionTypeAVMExport                  TransactionType = 0x4
	TransactionTypeAddValidator               TransactionType = 0xc
	TransactionTypeAddSubnetValidator         TransactionType = 0xd
	TransactionTypeAddDelegator               TransactionType = 0xe
	TransactionTypeCreateChain                TransactionType = 0xf
	TransactionTypeCreateSubnet               TransactionType = 0x10
	TransactionTypePVMImport                  TransactionType = 0x11
	TransactionTypePVMExport                  TransactionType = 0x12
	TransactionTypeAdvanceTime                TransactionType = 0x13
	TransactionTypeRewardValidator            TransactionType = 0x14
	TransactionTypeRemoveSubnetValidator      TransactionType = 0x15
	TransactionTypeTransformSubnet            TransactionType = 0x16
	TransactionTypeAddPermissionlessValidator TransactionType = 0x17
	TransactionTypeAddPermissionlessDelegator TransactionType = 0x18
	TransactionTypeAddDaoProposal             TransactionType = 0x19
	TransactionTypeAddDaoVote                 TransactionType = 0x20

	// Camino Custom Datatypes

	RegisterTransactionTypeCustom TransactionType = 8192
	RegisterOutputTypeCustom      OutputType      = 8192

	OutputTypesLockedOutD  OutputType = RegisterOutputTypeCustom + 0
	OutputTypesLockedOutB  OutputType = RegisterOutputTypeCustom + 1
	OutputTypesLockedOutDB OutputType = RegisterOutputTypeCustom + 2

	Secp256K1FxMultisigCredential  OutputType = RegisterOutputTypeCustom + 12
	MultisigAliasWithNonce         OutputType = RegisterOutputTypeCustom + 13
	Secp256K1FxCrossTransferOutput OutputType = RegisterOutputTypeCustom + 14

	TransactionTypeCaminoAddValidator    TransactionType = RegisterTransactionTypeCustom + 2
	TransactionTypeCaminoRewardValidator TransactionType = RegisterTransactionTypeCustom + 3
	TransactionTypeAddAddressState       TransactionType = RegisterTransactionTypeCustom + 4
	TransactionTypeDeposit               TransactionType = RegisterTransactionTypeCustom + 5
	TransactionTypeUnlockDeposit         TransactionType = RegisterTransactionTypeCustom + 6
	TransactionTypeRegisterNodeTx        TransactionType = RegisterTransactionTypeCustom + 7
	TransactionTypePvmBase               TransactionType = RegisterTransactionTypeCustom + 8
	TransactionTypeMultisigAlias         TransactionType = RegisterTransactionTypeCustom + 9
	TransactionTypeClaimReward           TransactionType = RegisterTransactionTypeCustom + 10
	TransactionTypeRewardsImport         TransactionType = RegisterTransactionTypeCustom + 11
	TransactionTypeAddDepositOffer       TransactionType = RegisterTransactionTypeCustom + 15

	ResultTypeTransaction SearchResultType = "transaction"
	ResultTypeAsset       SearchResultType = "asset"
	ResultTypeAddress     SearchResultType = "address"
	ResultTypeOutput      SearchResultType = "output"
	ResultTypeCBlock      SearchResultType = "cBlock"
	ResultTypeCTrans      SearchResultType = "cTransaction"
	ResultTypeCAddress    SearchResultType = "cAddress"

	TypeUnknown = "unknown"
)

// BlockType represents a sub class of Block.
type BlockType uint16

// TransactionType represents a sub class of Transaction.
type TransactionType uint16

func (t TransactionType) String() string {
	switch t {
	case TransactionTypeBase,
		TransactionTypePvmBase:
		return "base"

	// AVM
	case TransactionTypeCreateAsset:
		return "create_asset"
	case TransactionTypeOperation:
		return "operation"
	case TransactionTypeAVMImport:
		return "import"
	case TransactionTypeAVMExport:
		return "export"

		// PVM
	case TransactionTypeAddValidator,
		TransactionTypeCaminoAddValidator:
		return "add_validator"
	case TransactionTypeAddSubnetValidator:
		return "add_subnet_validator"
	case TransactionTypeAddDelegator:
		return "add_delegator"
	case TransactionTypeCreateChain:
		return "create_chain"
	case TransactionTypeCreateSubnet:
		return "create_subnet"
	case TransactionTypePVMImport:
		return "pvm_import"
	case TransactionTypePVMExport:
		return "pvm_export"
	case TransactionTypeAdvanceTime:
		return "advance_time"
	case TransactionTypeRewardValidator,
		TransactionTypeCaminoRewardValidator:
		return "reward_validator"
	case TransactionTypeRemoveSubnetValidator:
		return "remove_subnet_validator"
	case TransactionTypeTransformSubnet:
		return "transform_subnet"
	case TransactionTypeAddPermissionlessValidator:
		return "add_permissionless_validator"
	case TransactionTypeAddPermissionlessDelegator:
		return "add_permissionless_delegator"
	case TransactionTypeAddDaoProposal:
		return "add_dao_proposal"
	case TransactionTypeAddDaoVote:
		return "add_dao_vote"
	case TransactionTypeAddAddressState:
		return "address_state"
	case TransactionTypeDeposit:
		return "deposit"
	case TransactionTypeUnlockDeposit:
		return "unlock_deposit"
	case TransactionTypeRegisterNodeTx:
		return "register_node"
	case TransactionTypeMultisigAlias:
		return "multisig"
	case TransactionTypeClaimReward:
		return "claim"
	case TransactionTypeRewardsImport:
		return "rewards"
	case TransactionTypeAddDepositOffer:
		return "add_deposit_offer"
	default:
		return TypeUnknown
	}
}

// OutputType represents a sub class of Output.
type OutputType uint32

func (t OutputType) String() string {
	switch t {
	case OutputTypesSECP2556K1Transfer:
		return "secp256k1_transfer"
	case OutputTypesSECP2556K1Mint:
		return "secp256k1_mint"
	case OutputTypesNFTTransfer:
		return "nft_transfer"
	case OutputTypesNFTMint:
		return "nft_mint"
	case OutputTypesAtomicExportTx:
		return "atomic_export"
	case OutputTypesAtomicImportTx:
		return "atomic_import"
	case OutputTypesLockedOutD:
		return "deposit_locked"
	case OutputTypesLockedOutB:
		return "bond_locked"
	case OutputTypesLockedOutDB:
		return "deposit_bond_locked"
	case Secp256K1FxMultisigCredential:
		return "secp256k1_msig_cred"
	case MultisigAliasWithNonce:
		return "msig_alias"
	case Secp256K1FxCrossTransferOutput:
		return "secp256k1_xfer"
	default:
		return TypeUnknown
	}
}

// SearchResultType is the type for an object found from a search query.
type SearchResultType string
