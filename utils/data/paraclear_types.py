from dataclasses import dataclass, asdict
from typing import List
from starknet_py.cairo.felt import decode_shortstring, encode_shortstring
from starknet_py.cairo.felt import FIELD_PRIME

def as_int(value: int) -> int:
    """
    Returns the lift of the given field element, val, as an integer
    in the range (-prime/2, prime/2).
    """
    return value if value < FIELD_PRIME // 2 else value - FIELD_PRIME



@dataclass
class TokenAssetBalance:
    token_address: int
    amount: int
    prev: int
    next: int

    def to_dict(self):
        d = asdict(self)
        d['token_address'] = '0x' + hex(self.token_address)[2:].zfill(64)
        d['amount'] = as_int(self.amount)
        del d['prev']
        del d['next']
        return d

@dataclass
class PerpetualAssetBalance:
    market: int
    amount: int
    cost: int
    cached_funding: int
    prev: int
    next: int

    def to_dict(self):
        d = asdict(self)
        d['market'] = decode_shortstring(self.market)
        d['amount'] = as_int(self.amount)
        d['cost'] = as_int(self.cost)
        d['cached_funding'] = as_int(self.cached_funding)
        del d['prev']
        del d['next']
        return d

@dataclass
class PerpetualMarginParams:
    imf_base: int
    imf_factor: int
    mmf_factor: int
    imf_shift: int

    def to_dict(self):
        d = asdict(self)
        d['imf_base'] = as_int(self.imf_base)
        d['imf_factor'] = as_int(self.imf_factor)
        d['mmf_factor'] = as_int(self.mmf_factor)
        d['imf_shift'] = as_int(self.imf_shift)
        return d

@dataclass
class PerpetualAsset:
    market: int
    base_asset: int
    quote_asset: int
    tick_size: int
    margin_params: PerpetualMarginParams

    def to_dict(self):
        d = asdict(self)
        d['market'] = decode_shortstring(self.market)
        d['margin_params'] = self.margin_params.to_dict()
        return d

@dataclass
class PerpetualAssetBalanceDisplay:
    market: int
    amount: int
    cost: int
    cached_funding: int

    def to_dict(self):
        d = asdict(self)
        d['market'] = decode_shortstring(self.market)
        d['amount'] = as_int(self.amount)
        d['cost'] = as_int(self.cost)
        d['cached_funding'] = as_int(self.cached_funding)
        return d

@dataclass
class PerpetualAssetBalanceV2:
    market: int
    amount: int
    cost: int
    cached_funding: int
    trade_size: int
    trade_price: int

    def to_dict(self):
        d = asdict(self)
        d['market'] = decode_shortstring(self.market)
        d['amount'] = as_int(self.amount)
        d['cost'] = as_int(self.cost)
        d['cached_funding'] = as_int(self.cached_funding)
        d['trade_size'] = as_int(self.trade_size)
        d['trade_price'] = as_int(self.trade_price)
        return d

@dataclass
class Order:
    account: int
    market: int
    side: int
    type: int
    size: int
    price: int
    signature_timestamp: int

    def to_dict(self):
        d = asdict(self)
        d['market'] = decode_shortstring(self.market)
        d['size'] = as_int(self.size)
        d['price'] = as_int(self.price)
        return d

@dataclass
class TradeRequest:
    id: int
    size: int
    price: int
    market_price: int
    traded_at: int
    maker_order: Order
    taker_order: Order

    def to_dict(self):
        d = asdict(self)
        d['size'] = as_int(self.size)
        d['price'] = as_int(self.price)
        d['market_price'] = as_int(self.market_price)
        d['maker_order'] = self.maker_order.to_dict()
        d['taker_order'] = self.taker_order.to_dict()
        return d

@dataclass
class FeeRate:
    exists: int
    maker: int
    taker: int

    def to_dict(self):
        d = asdict(self)
        d['maker'] = as_int(self.maker)
        d['taker'] = as_int(self.taker)
        return d

@dataclass
class TokenAsset:
    initial_weight: int
    maintenance_weight: int
    conversion_weight: int
    tick_size: int
    token_address: int
    token_name: int

    def to_dict(self):
        d = asdict(self)
        d['token_address'] = '0x' + hex(self.token_address)[2:].zfill(64)
        d['token_name'] = decode_shortstring(self.token_name)
        return d

@dataclass
class Uint256:
    low: int
    high: int

    def to_dict(self):
        d = asdict(self)
        d['low'] = as_int(self.low)
        d['high'] = as_int(self.high)
        return d

@dataclass
class FeeShare:
    account: int
    fee: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['fee'] = as_int(self.fee)
        return d

@dataclass
class AccountStateEmitted:
    account: int
    tokens_len: int
    tokens: List[TokenAssetBalance]
    synthetics_len: int
    synthetics: List[PerpetualAssetBalance]

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['tokens'] = [token.to_dict() for token in self.tokens]
        d['synthetics'] = [synth.to_dict() for synth in self.synthetics]
        return d

@dataclass
class AccountReferralUpdate:
    account: int
    referrer: int
    fee_commission: int
    fee_discount: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['referrer'] = '0x' + hex(self.referrer)[2:].zfill(64)
        d['fee_commission'] = as_int(self.fee_commission)
        d['fee_discount'] = as_int(self.fee_discount)
        return d

@dataclass
class FeeShareUpdate:
    fee_share_account: int
    fee_share_d: int
    previous_fee_share_account: int
    previous_fee_share_d: int

    def to_dict(self):
        d = asdict(self)
        d['fee_share_account'] = '0x' + hex(self.fee_share_account)[2:].zfill(64)
        d['fee_share_d'] = as_int(self.fee_share_d)
        d['previous_fee_share_account'] = '0x' + hex(self.previous_fee_share_account)[2:].zfill(64)
        d['previous_fee_share_d'] = as_int(self.previous_fee_share_d)
        return d

@dataclass
class TokenAssetBalanceUpdate:
    account: int
    token_address: int
    prev_amount: int
    updated_amount: int
    is_liquidation: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['token_address'] = '0x' + hex(self.token_address)[2:].zfill(64)
        d['prev_amount'] = as_int(self.prev_amount)
        d['updated_amount'] = as_int(self.updated_amount)
        return d

@dataclass
class RealizedFunding:
    account: int
    market: int
    realized_funding: int
    balance_amount: int
    prev_funding: int
    current_funding: int
    block_timestamp: int
    trade_id: int
    settlement_token_asset_price: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['market'] = decode_shortstring(self.market)
        d['realized_funding'] = as_int(self.realized_funding)
        d['balance_amount'] = as_int(self.balance_amount)
        d['prev_funding'] = as_int(self.prev_funding)
        d['current_funding'] = as_int(self.current_funding)
        d['settlement_token_asset_price'] = as_int(self.settlement_token_asset_price)
        d['trade_id'] = str(self.trade_id)
        return d

@dataclass
class RealizedPNL:
    account: int
    realized_pnl: int
    block_timestamp: int
    settlement_token_asset_price: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['realized_pnl'] = as_int(self.realized_pnl)
        d['settlement_token_asset_price'] = as_int(self.settlement_token_asset_price)
        return d

@dataclass
class AccountLiquidated:
    account: int
    liquidator: int
    token_assets_value: int
    margin_requirement: int
    unrealized_pnl: int
    liquidation_penalty: int
    liquidation_share: int
    is_partial_liquidation: int
    oracle_snapshot_id: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['liquidator'] = '0x' + hex(self.liquidator)[2:].zfill(64)
        d['token_assets_value'] = as_int(self.token_assets_value)
        d['margin_requirement'] = as_int(self.margin_requirement)
        d['unrealized_pnl'] = as_int(self.unrealized_pnl)
        d['liquidation_penalty'] = as_int(self.liquidation_penalty)
        d['liquidation_share'] = as_int(self.liquidation_share)
        d['oracle_snapshot_id'] = str(self.oracle_snapshot_id)
        return d

@dataclass
class Deposit:
    account: int
    token_address: int
    amount: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['token_address'] = '0x' + hex(self.token_address)[2:].zfill(64)
        d['amount'] = as_int(self.amount)
        return d

@dataclass
class Withdraw:
    account: int
    token_address: int
    amount: int
    socialized_loss_factor: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['token_address'] = '0x' + hex(self.token_address)[2:].zfill(64)
        d['amount'] = as_int(self.amount)
        d['socialized_loss_factor'] = as_int(self.socialized_loss_factor)
        return d

@dataclass
class Transfer:
    sender: int
    recipient: int
    token_address: int
    amount: int

    def to_dict(self):
        d = asdict(self)
        d['sender'] = '0x' + hex(self.sender)[2:].zfill(64)
        d['recipient'] = '0x' + hex(self.recipient)[2:].zfill(64)
        d['token_address'] = '0x' + hex(self.token_address)[2:].zfill(64)
        d['amount'] = as_int(self.amount)
        return d

@dataclass
class AccountTransfer:
    account: int
    receiver: int
    share: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['receiver'] = '0x' + hex(self.receiver)[2:].zfill(64)
        d['share'] = as_int(self.share)
        return d

@dataclass
class PositionSettled:
    settle_id: int
    account: int
    market: int
    price: int
    size: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['market'] = decode_shortstring(self.market)
        d['price'] = as_int(self.price)
        d['size'] = as_int(self.size)
        return d

@dataclass
class SettleTradeFailed:
    error_code: int
    error_message: int
    trade: TradeRequest

    def to_dict(self):
        d = asdict(self)
        d['error_message'] = decode_shortstring(self.error_message)
        d['trade'] = self.trade.to_dict()
        return d

@dataclass
class LiquidationFailed:
    error_code: int
    error_message: int
    account: int

    def to_dict(self):
        d = asdict(self)
        d['error_message'] = decode_shortstring(self.error_message)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        return d

@dataclass
class RoleGranted:
    role: int
    account: int
    sender: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['sender'] = '0x' + hex(self.sender)[2:].zfill(64)
        return d

@dataclass
class RoleRevoked:
    role: int
    account: int
    sender: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['sender'] = '0x' + hex(self.sender)[2:].zfill(64)
        return d

@dataclass
class RoleAdminChanged:
    role: int
    previousAdminRole: int
    newAdminRole: int

    def to_dict(self):
        return asdict(self)

@dataclass
class Upgraded:
    implementation: int

    def to_dict(self):
        d = asdict(self)
        d['implementation'] = '0x' + hex(self.implementation)[2:].zfill(64)
        return d

@dataclass
class AdminChanged:
    previousAdmin: int
    newAdmin: int

    def to_dict(self):
        d = asdict(self)
        d['previousAdmin'] = '0x' + hex(self.previousAdmin)[2:].zfill(64)
        d['newAdmin'] = '0x' + hex(self.newAdmin)[2:].zfill(64)
        return d

@dataclass
class TradeSettled:
    trade_id: int
    market: int
    price: int
    size: int

    def to_dict(self):
        d = asdict(self)
        d['market'] = decode_shortstring(self.market)
        d['price'] = as_int(self.price)
        d['size'] = as_int(self.size)
        d['trade_id'] = str(self.trade_id)
        return d

@dataclass
class PerpetualAssetUpdated:
    updated_asset: PerpetualAsset

    def to_dict(self):
        d = asdict(self)
        d['updated_asset'] = self.updated_asset.to_dict()
        return d

@dataclass
class PerpetualAssetBalanceUpdate:
    trade_id: int
    account: int
    updated_asset_balance: PerpetualAssetBalanceDisplay

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        if not isinstance(self.updated_asset_balance, PerpetualAssetBalanceDisplay):
            self.updated_asset_balance = PerpetualAssetBalanceDisplay(**self.updated_asset_balance)
        d['updated_asset_balance'] = self.updated_asset_balance.to_dict()
        d['trade_id'] = str(self.trade_id)
        return d

@dataclass
class PerpetualAssetBalanceUpdateV2:
    trade_id: int
    account: int
    updated_asset_balance: PerpetualAssetBalanceV2

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['updated_asset_balance'] = self.updated_asset_balance.to_dict()
        d['trade_id'] = str(self.trade_id)
        return d

@dataclass
class Fee:
    account: int
    fee: int

    def to_dict(self):
        d = asdict(self)
        d['account'] = '0x' + hex(self.account)[2:].zfill(64)
        d['fee'] = as_int(self.fee)
        return d