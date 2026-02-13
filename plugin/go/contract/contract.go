package contract

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/rand"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

/* This file contains the base contract implementation that overrides the basic 'transfer' functionality */

// PluginConfig: the configuration of the contract
var ContractConfig = &PluginConfig{
	Name:                  "go_plugin_contract",
	Id:                    1,
	Version:               1,
	SupportedTransactions: []string{"send", "reward", "stake", "unstake"},
	TransactionTypeUrls: []string{
		"type.googleapis.com/types.MessageSend",
		"type.googleapis.com/types.MessageReward",
		"type.googleapis.com/types.MessageStake",
		"type.googleapis.com/types.MessageUnstake",
	},
	EventTypeUrls: nil,
}

// init sets FileDescriptorProtos after ensuring .pb.go files are initialized
func init() {
	// Explicitly initialize the proto files first to ensure File_*_proto are set
	file_account_proto_init()
	file_event_proto_init()
	file_plugin_proto_init()
	file_tx_proto_init()

	var fds [][]byte
	// Include google/protobuf/any.proto first as it's a dependency of event.proto and tx.proto
	for _, file := range []protoreflect.FileDescriptor{
		anypb.File_google_protobuf_any_proto,
		File_account_proto, File_event_proto, File_plugin_proto, File_tx_proto,
	} {
		fd, _ := proto.Marshal(protodesc.ToFileDescriptorProto(file))
		fds = append(fds, fd)
	}
	ContractConfig.FileDescriptorProtos = fds
}

// Contract() defines the smart contract that implements the extended logic of the nested chain
type Contract struct {
	Config    Config
	FSMConfig *PluginFSMConfig // fsm configuration
	plugin    *Plugin          // plugin connection
	fsmId     uint64           // the id of the requesting fsm
}

// Genesis() implements logic to import a json file to create the state at height 0 and export the state at any height
func (c *Contract) Genesis(_ *PluginGenesisRequest) *PluginGenesisResponse {
	return &PluginGenesisResponse{} // TODO map out original token holders
}

// BeginBlock() is code that is executed at the start of `applying` the block
func (c *Contract) BeginBlock(_ *PluginBeginRequest) *PluginBeginResponse {
	return &PluginBeginResponse{}
}

// CheckTx() is code that is executed to statelessly validate a transaction
func (c *Contract) CheckTx(request *PluginCheckRequest) *PluginCheckResponse {
	// validate fee
	resp, err := c.plugin.StateRead(c, &PluginStateReadRequest{
		Keys: []*PluginKeyRead{
			{QueryId: rand.Uint64(), Key: KeyForFeeParams()},
		}})
	if err == nil {
		err = resp.Error
	}
	// handle error
	if err != nil {
		return &PluginCheckResponse{Error: err}
	}
	// convert bytes into fee parameters
	minFees := new(FeeParams)
	if err = Unmarshal(resp.Results[0].Entries[0].Value, minFees); err != nil {
		return &PluginCheckResponse{Error: err}
	}
	// check for the minimum fee
	if request.Tx.Fee < minFees.SendFee {
		return &PluginCheckResponse{Error: ErrTxFeeBelowStateLimit()}
	}
	// get the message
	msg, err := FromAny(request.Tx.Msg)
	if err != nil {
		return &PluginCheckResponse{Error: err}
	}
	// handle the message
	switch x := msg.(type) {
	case *MessageSend:
		return c.CheckMessageSend(x)
	case *MessageReward:
		return c.CheckMessageReward(x)
	case *MessageStake:
		return c.CheckMessageStake(x)
	case *MessageUnstake:
		return c.CheckMessageUnstake(x)
	default:
		return &PluginCheckResponse{Error: ErrInvalidMessageCast()}
	}
}

// DeliverTx() is code that is executed to apply a transaction
func (c *Contract) DeliverTx(request *PluginDeliverRequest) *PluginDeliverResponse {
	// get the message
	msg, err := FromAny(request.Tx.Msg)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	// handle the message
	switch x := msg.(type) {
	case *MessageSend:
		return c.DeliverMessageSend(x, request.Tx.Fee)
	case *MessageReward:
		return c.DeliverMessageReward(x, request.Tx.Fee)
	case *MessageStake:
		return c.DeliverMessageStake(x, request.Tx.Fee)
	case *MessageUnstake:
		return c.DeliverMessageUnstake(x, request.Tx.Fee)
	default:
		return &PluginDeliverResponse{Error: ErrInvalidMessageCast()}
	}
}

// EndBlock() is code that is executed at the end of 'applying' a block
func (c *Contract) EndBlock(_ *PluginEndRequest) *PluginEndResponse {
	return &PluginEndResponse{}
}

// CheckMessageSend() statelessly validates a 'send' message
func (c *Contract) CheckMessageSend(msg *MessageSend) *PluginCheckResponse {
	// check sender address
	if len(msg.FromAddress) != 20 {
		return &PluginCheckResponse{Error: ErrInvalidAddress()}
	}
	// check recipient address
	if len(msg.ToAddress) != 20 {
		return &PluginCheckResponse{Error: ErrInvalidAddress()}
	}
	// check amount
	if msg.Amount == 0 {
		return &PluginCheckResponse{Error: ErrInvalidAmount()}
	}
	// return the authorized signers
	return &PluginCheckResponse{Recipient: msg.ToAddress, AuthorizedSigners: [][]byte{msg.FromAddress}}
}

// CheckMessageReward() statelessly validates a 'reward' message
func (c *Contract) CheckMessageReward(msg *MessageReward) *PluginCheckResponse {
	// check admin address
	if len(msg.AdminAddress) != 20 {
		return &PluginCheckResponse{Error: ErrInvalidAddress()}
	}
	// check recipient address
	if len(msg.RecipientAddress) != 20 {
		return &PluginCheckResponse{Error: ErrInvalidAddress()}
	}
	// check amount
	if msg.Amount == 0 {
		return &PluginCheckResponse{Error: ErrInvalidAmount()}
	}
	// return the authorized signers (admin must sign)
	return &PluginCheckResponse{Recipient: msg.RecipientAddress, AuthorizedSigners: [][]byte{msg.AdminAddress}}
}

// CheckMessageStake() statelessly validates a 'stake' message
func (c *Contract) CheckMessageStake(msg *MessageStake) *PluginCheckResponse {
	// check sender address
	if len(msg.SenderAddress) != 20 {
		return &PluginCheckResponse{Error: ErrInvalidAddress()}
	}
	// check amount
	if msg.Amount == 0 {
		return &PluginCheckResponse{Error: ErrInvalidAmount()}
	}
	// return the authorized signers (sender must sign)
	return &PluginCheckResponse{AuthorizedSigners: [][]byte{msg.SenderAddress}}
}

// CheckMessageUnstake() statelessly validates an 'unstake' message
func (c *Contract) CheckMessageUnstake(msg *MessageUnstake) *PluginCheckResponse {
	// check sender address
	if len(msg.SenderAddress) != 20 {
		return &PluginCheckResponse{Error: ErrInvalidAddress()}
	}
	// check amount
	if msg.Amount == 0 {
		return &PluginCheckResponse{Error: ErrInvalidAmount()}
	}
	// return the authorized signers (sender must sign)
	return &PluginCheckResponse{AuthorizedSigners: [][]byte{msg.SenderAddress}}
}

// DeliverMessageSend() handles a 'send' message
func (c *Contract) DeliverMessageSend(msg *MessageSend, fee uint64) *PluginDeliverResponse {
	log.Printf("DeliverMessageSend called: from=%x to=%x amount=%d fee=%d", msg.FromAddress, msg.ToAddress, msg.Amount, fee)
	var (
		fromKey, toKey, feePoolKey         []byte
		fromBytes, toBytes, feePoolBytes   []byte
		fromQueryId, toQueryId, feeQueryId = rand.Uint64(), rand.Uint64(), rand.Uint64()
		from, to, feePool                  = new(Account), new(Account), new(Pool)
	)
	// calculate the from key and to key
	fromKey, toKey, feePoolKey = KeyForAccount(msg.FromAddress), KeyForAccount(msg.ToAddress), KeyForFeePool(c.Config.ChainId)
	log.Printf("Keys: fromKey=%x toKey=%x feePoolKey=%x", fromKey, toKey, feePoolKey)
	// get the from and to account
	response, err := c.plugin.StateRead(c, &PluginStateReadRequest{
		Keys: []*PluginKeyRead{
			{QueryId: feeQueryId, Key: feePoolKey},
			{QueryId: fromQueryId, Key: fromKey},
			{QueryId: toQueryId, Key: toKey},
		}})
	// check for internal error
	if err != nil {
		log.Printf("StateRead error: %v", err)
		return &PluginDeliverResponse{Error: err}
	}
	// ensure no error fsm error
	if response.Error != nil {
		log.Printf("StateRead FSM error: %v", response.Error)
		return &PluginDeliverResponse{Error: response.Error}
	}
	log.Printf("StateRead returned %d results", len(response.Results))
	// get the from bytes and to bytes
	for _, resp := range response.Results {
		log.Printf("Result QueryId=%d Entries=%d", resp.QueryId, len(resp.Entries))
		if len(resp.Entries) == 0 {
			log.Printf("WARNING: No entries for QueryId=%d", resp.QueryId)
			continue
		}
		switch resp.QueryId {
		case fromQueryId:
			fromBytes = resp.Entries[0].Value
			log.Printf("fromBytes len=%d", len(fromBytes))
		case toQueryId:
			toBytes = resp.Entries[0].Value
			log.Printf("toBytes len=%d", len(toBytes))
		case feeQueryId:
			feePoolBytes = resp.Entries[0].Value
			log.Printf("feePoolBytes len=%d", len(feePoolBytes))
		}
	}
	// add fee to 'amount to deduct'
	amountToDeduct := msg.Amount + fee
	// convert the bytes to account structures
	if err = Unmarshal(fromBytes, from); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	if err = Unmarshal(toBytes, to); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	if err = Unmarshal(feePoolBytes, feePool); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	log.Printf("from.Amount=%d to.Amount=%d feePool.Amount=%d", from.Amount, to.Amount, feePool.Amount)
	// if the account amount is less than the amount to subtract; return insufficient funds
	if from.Amount < amountToDeduct {
		log.Printf("ERROR: Insufficient funds: from.Amount=%d amountToDeduct=%d", from.Amount, amountToDeduct)
		return &PluginDeliverResponse{Error: ErrInsufficientFunds()}
	}
	// for self-transfer, use same account data
	if bytes.Equal(fromKey, toKey) {
		to = from
	}
	// subtract from sender
	from.Amount -= amountToDeduct
	// add the fee to the 'fee pool'
	feePool.Amount += fee
	// add to recipient
	to.Amount += msg.Amount
	log.Printf("AFTER: from.Amount=%d to.Amount=%d feePool.Amount=%d", from.Amount, to.Amount, feePool.Amount)
	// convert the accounts to bytes
	fromBytes, err = Marshal(from)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	toBytes, err = Marshal(to)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	feePoolBytes, err = Marshal(feePool)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	// execute writes to the database
	var resp *PluginStateWriteResponse
	// if the from account is drained - delete the from account
	if from.Amount == 0 {
		resp, err = c.plugin.StateWrite(c, &PluginStateWriteRequest{
			Sets: []*PluginSetOp{
				{Key: feePoolKey, Value: feePoolBytes},
				{Key: toKey, Value: toBytes},
			},
			Deletes: []*PluginDeleteOp{{Key: fromKey}},
		})
	} else {
		resp, err = c.plugin.StateWrite(c, &PluginStateWriteRequest{
			Sets: []*PluginSetOp{
				{Key: feePoolKey, Value: feePoolBytes},
				{Key: toKey, Value: toBytes},
				{Key: fromKey, Value: fromBytes},
			},
		})
	}
	if err != nil {
		log.Printf("StateWrite internal error: %v", err)
		return &PluginDeliverResponse{Error: err}
	}
	if resp.Error != nil {
		log.Printf("StateWrite FSM error: %v", resp.Error)
		return &PluginDeliverResponse{Error: resp.Error}
	}
	log.Printf("StateWrite SUCCESS!")
	return &PluginDeliverResponse{}
}

// DeliverMessageReward() handles a 'reward' message (minting transaction)
// This creates new tokens for a recipient. The admin must pay a fee but does not lose tokens.
func (c *Contract) DeliverMessageReward(msg *MessageReward, fee uint64) *PluginDeliverResponse {
	log.Printf("DeliverMessageReward called: admin=%x recipient=%x amount=%d fee=%d", msg.AdminAddress, msg.RecipientAddress, msg.Amount, fee)
	var (
		adminKey, recipientKey, feePoolKey         []byte
		adminBytes, recipientBytes, feePoolBytes   []byte
		adminQueryId, recipientQueryId, feeQueryId = rand.Uint64(), rand.Uint64(), rand.Uint64()
		admin, recipient, feePool                  = new(Account), new(Account), new(Pool)
	)
	// calculate the admin key, recipient key, and fee pool key
	adminKey, recipientKey, feePoolKey = KeyForAccount(msg.AdminAddress), KeyForAccount(msg.RecipientAddress), KeyForFeePool(c.Config.ChainId)
	log.Printf("Keys: adminKey=%x recipientKey=%x feePoolKey=%x", adminKey, recipientKey, feePoolKey)
	// read admin, recipient, and fee pool accounts
	response, err := c.plugin.StateRead(c, &PluginStateReadRequest{
		Keys: []*PluginKeyRead{
			{QueryId: adminQueryId, Key: adminKey},
			{QueryId: recipientQueryId, Key: recipientKey},
			{QueryId: feeQueryId, Key: feePoolKey},
		}})
	// check for internal error
	if err != nil {
		log.Printf("StateRead error: %v", err)
		return &PluginDeliverResponse{Error: err}
	}
	// ensure no FSM error
	if response.Error != nil {
		log.Printf("StateRead FSM error: %v", response.Error)
		return &PluginDeliverResponse{Error: response.Error}
	}
	log.Printf("StateRead returned %d results", len(response.Results))
	// get the admin bytes, recipient bytes, and fee pool bytes
	for _, resp := range response.Results {
		log.Printf("Result QueryId=%d Entries=%d", resp.QueryId, len(resp.Entries))
		if len(resp.Entries) == 0 {
			log.Printf("WARNING: No entries for QueryId=%d", resp.QueryId)
			continue
		}
		switch resp.QueryId {
		case adminQueryId:
			adminBytes = resp.Entries[0].Value
			log.Printf("adminBytes len=%d", len(adminBytes))
		case recipientQueryId:
			recipientBytes = resp.Entries[0].Value
			log.Printf("recipientBytes len=%d", len(recipientBytes))
		case feeQueryId:
			feePoolBytes = resp.Entries[0].Value
			log.Printf("feePoolBytes len=%d", len(feePoolBytes))
		}
	}
	// convert the bytes to account structures
	if err = Unmarshal(adminBytes, admin); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	if err = Unmarshal(recipientBytes, recipient); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	if err = Unmarshal(feePoolBytes, feePool); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	log.Printf("admin.Amount=%d recipient.Amount=%d feePool.Amount=%d", admin.Amount, recipient.Amount, feePool.Amount)
	// admin must have enough to pay the fee
	if admin.Amount < fee {
		log.Printf("ERROR: Insufficient funds for fee: admin.Amount=%d fee=%d", admin.Amount, fee)
		return &PluginDeliverResponse{Error: ErrInsufficientFunds()}
	}
	// for self-transfer, use same account data
	if bytes.Equal(adminKey, recipientKey) {
		recipient = admin
	}
	// deduct fee from admin
	admin.Amount -= fee
	// add the fee to the 'fee pool'
	feePool.Amount += fee
	// mint tokens to recipient (no deduction from admin for the minted amount)
	recipient.Amount += msg.Amount
	log.Printf("AFTER: admin.Amount=%d recipient.Amount=%d feePool.Amount=%d", admin.Amount, recipient.Amount, feePool.Amount)
	// convert the accounts to bytes
	adminBytes, err = Marshal(admin)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	recipientBytes, err = Marshal(recipient)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	feePoolBytes, err = Marshal(feePool)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	// execute writes to the database
	var resp *PluginStateWriteResponse
	// if the admin account is drained - delete the admin account
	if admin.Amount == 0 {
		resp, err = c.plugin.StateWrite(c, &PluginStateWriteRequest{
			Sets: []*PluginSetOp{
				{Key: feePoolKey, Value: feePoolBytes},
				{Key: recipientKey, Value: recipientBytes},
			},
			Deletes: []*PluginDeleteOp{{Key: adminKey}},
		})
	} else {
		resp, err = c.plugin.StateWrite(c, &PluginStateWriteRequest{
			Sets: []*PluginSetOp{
				{Key: feePoolKey, Value: feePoolBytes},
				{Key: recipientKey, Value: recipientBytes},
				{Key: adminKey, Value: adminBytes},
			},
		})
	}
	if err != nil {
		log.Printf("StateWrite internal error: %v", err)
		return &PluginDeliverResponse{Error: err}
	}
	if resp.Error != nil {
		log.Printf("StateWrite FSM error: %v", resp.Error)
		return &PluginDeliverResponse{Error: resp.Error}
	}
	log.Printf("StateWrite SUCCESS!")
	return &PluginDeliverResponse{}
}

// DeliverMessageStake() handles a 'stake' message
// This locks tokens into a staking pool, removing them from the sender's available balance
func (c *Contract) DeliverMessageStake(msg *MessageStake, fee uint64) *PluginDeliverResponse {
	log.Printf("DeliverMessageStake called: sender=%x amount=%d fee=%d", msg.SenderAddress, msg.Amount, fee)
	var (
		senderKey, stakeKey, feePoolKey              []byte
		senderBytes, stakeBytes, feePoolBytes        []byte
		senderQueryId, stakeQueryId, feeQueryId      = rand.Uint64(), rand.Uint64(), rand.Uint64()
		sender, feePool, stake                       = new(Account), new(Pool), new(Stake)
	)
	// calculate the keys
	senderKey, stakeKey, feePoolKey = KeyForAccount(msg.SenderAddress), KeyForStake(msg.SenderAddress), KeyForFeePool(c.Config.ChainId)
	log.Printf("Keys: senderKey=%x stakeKey=%x feePoolKey=%x", senderKey, stakeKey, feePoolKey)
	// read current state
	response, err := c.plugin.StateRead(c, &PluginStateReadRequest{
		Keys: []*PluginKeyRead{
			{QueryId: senderQueryId, Key: senderKey},
			{QueryId: stakeQueryId, Key: stakeKey},
			{QueryId: feeQueryId, Key: feePoolKey},
		}})
	// check for internal error
	if err != nil {
		log.Printf("StateRead error: %v", err)
		return &PluginDeliverResponse{Error: err}
	}
	// ensure no FSM error
	if response.Error != nil {
		log.Printf("StateRead FSM error: %v", response.Error)
		return &PluginDeliverResponse{Error: response.Error}
	}
	log.Printf("StateRead returned %d results", len(response.Results))
	// get bytes from results
	for _, resp := range response.Results {
		log.Printf("Result QueryId=%d Entries=%d", resp.QueryId, len(resp.Entries))
		if len(resp.Entries) == 0 {
			log.Printf("WARNING: No entries for QueryId=%d", resp.QueryId)
			continue
		}
		switch resp.QueryId {
		case senderQueryId:
			senderBytes = resp.Entries[0].Value
		case stakeQueryId:
			stakeBytes = resp.Entries[0].Value
		case feeQueryId:
			feePoolBytes = resp.Entries[0].Value
		}
	}
	// unmarshal accounts and stake
	if err = Unmarshal(senderBytes, sender); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	if err = Unmarshal(stakeBytes, stake); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	if err = Unmarshal(feePoolBytes, feePool); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	log.Printf("sender.Amount=%d stake.Amount=%d feePool.Amount=%d", sender.Amount, stake.Amount, feePool.Amount)
	// sender must have enough to pay fee + stake amount
	amountToDeduct := msg.Amount + fee
	if sender.Amount < amountToDeduct {
		log.Printf("ERROR: Insufficient funds: sender.Amount=%d amountToDeduct=%d", sender.Amount, amountToDeduct)
		return &PluginDeliverResponse{Error: ErrInsufficientFunds()}
	}
	// apply state changes
	sender.Amount -= amountToDeduct  // deduct stake amount + fee from balance
	stake.Amount += msg.Amount        // add to staked amount
	stake.Address = msg.SenderAddress // ensure address is set
	feePool.Amount += fee             // add fee to pool
	log.Printf("AFTER: sender.Amount=%d stake.Amount=%d feePool.Amount=%d", sender.Amount, stake.Amount, feePool.Amount)
	// marshal updated state
	senderBytes, err = Marshal(sender)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	stakeBytes, err = Marshal(stake)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	feePoolBytes, err = Marshal(feePool)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	// execute writes to the database
	var resp *PluginStateWriteResponse
	// if sender account is drained - delete it
	if sender.Amount == 0 {
		resp, err = c.plugin.StateWrite(c, &PluginStateWriteRequest{
			Sets: []*PluginSetOp{
				{Key: stakeKey, Value: stakeBytes},
				{Key: feePoolKey, Value: feePoolBytes},
			},
			Deletes: []*PluginDeleteOp{{Key: senderKey}},
		})
	} else {
		resp, err = c.plugin.StateWrite(c, &PluginStateWriteRequest{
			Sets: []*PluginSetOp{
				{Key: feePoolKey, Value: feePoolBytes},
				{Key: senderKey, Value: senderBytes},
				{Key: stakeKey, Value: stakeBytes},
			},
		})
	}
	if err != nil {
		log.Printf("StateWrite internal error: %v", err)
		return &PluginDeliverResponse{Error: err}
	}
	if resp.Error != nil {
		log.Printf("StateWrite FSM error: %v", resp.Error)
		return &PluginDeliverResponse{Error: resp.Error}
	}
	log.Printf("StateWrite SUCCESS!")
	return &PluginDeliverResponse{}
}

// DeliverMessageUnstake() handles an 'unstake' message
// This removes tokens from the staking pool and returns them to the sender's balance
func (c *Contract) DeliverMessageUnstake(msg *MessageUnstake, fee uint64) *PluginDeliverResponse {
	log.Printf("DeliverMessageUnstake called: sender=%x amount=%d fee=%d", msg.SenderAddress, msg.Amount, fee)
	var (
		senderKey, stakeKey, feePoolKey              []byte
		senderBytes, stakeBytes, feePoolBytes        []byte
		senderQueryId, stakeQueryId, feeQueryId      = rand.Uint64(), rand.Uint64(), rand.Uint64()
		sender, feePool, stake                       = new(Account), new(Pool), new(Stake)
	)
	// calculate the keys
	senderKey, stakeKey, feePoolKey = KeyForAccount(msg.SenderAddress), KeyForStake(msg.SenderAddress), KeyForFeePool(c.Config.ChainId)
	log.Printf("Keys: senderKey=%x stakeKey=%x feePoolKey=%x", senderKey, stakeKey, feePoolKey)
	// read current state
	response, err := c.plugin.StateRead(c, &PluginStateReadRequest{
		Keys: []*PluginKeyRead{
			{QueryId: senderQueryId, Key: senderKey},
			{QueryId: stakeQueryId, Key: stakeKey},
			{QueryId: feeQueryId, Key: feePoolKey},
		}})
	// check for internal error
	if err != nil {
		log.Printf("StateRead error: %v", err)
		return &PluginDeliverResponse{Error: err}
	}
	// ensure no FSM error
	if response.Error != nil {
		log.Printf("StateRead FSM error: %v", response.Error)
		return &PluginDeliverResponse{Error: response.Error}
	}
	log.Printf("StateRead returned %d results", len(response.Results))
	// get bytes from results
	for _, resp := range response.Results {
		log.Printf("Result QueryId=%d Entries=%d", resp.QueryId, len(resp.Entries))
		if len(resp.Entries) == 0 {
			log.Printf("WARNING: No entries for QueryId=%d", resp.QueryId)
			continue
		}
		switch resp.QueryId {
		case senderQueryId:
			senderBytes = resp.Entries[0].Value
		case stakeQueryId:
			stakeBytes = resp.Entries[0].Value
		case feeQueryId:
			feePoolBytes = resp.Entries[0].Value
		}
	}
	// unmarshal accounts and stake
	if err = Unmarshal(senderBytes, sender); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	if err = Unmarshal(stakeBytes, stake); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	if err = Unmarshal(feePoolBytes, feePool); err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	log.Printf("sender.Amount=%d stake.Amount=%d feePool.Amount=%d", sender.Amount, stake.Amount, feePool.Amount)
	// sender must have sufficient staked amount and balance for fee
	if stake.Amount < msg.Amount {
		log.Printf("ERROR: Insufficient staked tokens: stake.Amount=%d unstake.Amount=%d", stake.Amount, msg.Amount)
		return &PluginDeliverResponse{Error: ErrInsufficientFunds()}
	}
	if sender.Amount < fee {
		log.Printf("ERROR: Insufficient balance for fee: sender.Amount=%d fee=%d", sender.Amount, fee)
		return &PluginDeliverResponse{Error: ErrInsufficientFunds()}
	}
	// apply state changes
	sender.Amount -= fee              // pay fee from balance
	stake.Amount -= msg.Amount        // remove from staked amount
	sender.Amount += msg.Amount       // add unstaked tokens back to balance
	feePool.Amount += fee             // add fee to pool
	log.Printf("AFTER: sender.Amount=%d stake.Amount=%d feePool.Amount=%d", sender.Amount, stake.Amount, feePool.Amount)
	// marshal updated state
	senderBytes, err = Marshal(sender)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	feePoolBytes, err = Marshal(feePool)
	if err != nil {
		return &PluginDeliverResponse{Error: err}
	}
	// execute writes to the database
	var resp *PluginStateWriteResponse
	// if stake is fully unstaked - delete the stake entry
	if stake.Amount == 0 {
		resp, err = c.plugin.StateWrite(c, &PluginStateWriteRequest{
			Sets: []*PluginSetOp{
				{Key: feePoolKey, Value: feePoolBytes},
				{Key: senderKey, Value: senderBytes},
			},
			Deletes: []*PluginDeleteOp{{Key: stakeKey}},
		})
	} else {
		stakeBytes, err = Marshal(stake)
		if err != nil {
			return &PluginDeliverResponse{Error: err}
		}
		resp, err = c.plugin.StateWrite(c, &PluginStateWriteRequest{
			Sets: []*PluginSetOp{
				{Key: feePoolKey, Value: feePoolBytes},
				{Key: senderKey, Value: senderBytes},
				{Key: stakeKey, Value: stakeBytes},
			},
		})
	}
	if err != nil {
		log.Printf("StateWrite internal error: %v", err)
		return &PluginDeliverResponse{Error: err}
	}
	if resp.Error != nil {
		log.Printf("StateWrite FSM error: %v", resp.Error)
		return &PluginDeliverResponse{Error: resp.Error}
	}
	log.Printf("StateWrite SUCCESS!")
	return &PluginDeliverResponse{}
}

var (
	accountPrefix = []byte{1} // store key prefix for accounts
	poolPrefix    = []byte{2} // store key prefix for pools
	stakePrefix   = []byte{3} // store key prefix for staking
	paramsPrefix  = []byte{7} // store key prefix for governance parameters
)

// KeyForAccount() returns the state database key for an account
func KeyForAccount(addr []byte) []byte {
	return JoinLenPrefix(accountPrefix, addr)
}

// KeyForStake() returns the state database key for a staker's staking information
func KeyForStake(addr []byte) []byte {
	return JoinLenPrefix(stakePrefix, addr)
}

// KeyForFeeParams() returns the state database key for governance controlled 'fee parameters'
func KeyForFeeParams() []byte {
	return JoinLenPrefix(paramsPrefix, []byte("/f/"))
}

// KeyForFeeParams() returns the state database key for governance controlled 'fee parameters'
func KeyForFeePool(chainId uint64) []byte {
	return JoinLenPrefix(poolPrefix, formatUint64(chainId))
}

func formatUint64(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}
