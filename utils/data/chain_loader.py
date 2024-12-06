from typing import Dict, List, Tuple, Callable, Any, Optional
import duckdb
from httpx import Client
from starknet_py.net.client_errors import ClientError
from starknet_py.constants import RPC_CONTRACT_ERROR

from dataclasses import make_dataclass
import os
import re
import sys

import pandas as pd
from paradex_py.api.api_client import ParadexApiClient
from paradex_py.api.models import SystemConfig
from paradex_py.environment import PROD
from starknet_py.net.full_node_client import FullNodeClient
from starknet_py.contract import Contract
from starknet_py.net.client_models import TransactionTrace, InvokeTransactionTrace
from starknet_py.proxy.contract_abi_resolver import ProxyConfig
from starknet_py.proxy.proxy_check import ArgentProxyCheck, OpenZeppelinProxyCheck, ProxyCheck
from starknet_py.net.models import Address, AddressRepresentation, InvokeV1, parse_address
from starknet_py.cairo.felt import decode_shortstring, encode_shortstring

from starknet_py.hash.selector import get_selector_from_name
from starknet_py.net.client_models import Call
from starknet_py.hash.address import get_checksum_address

from pathlib import Path
from dataclasses import dataclass, field
import logging
import asyncio
from starknet_py.abi.v2 import AbiParser
from starknet_py.serialization import serializer_for_event
from collections import defaultdict
from utils.data.paraclear_types import *

# Create module logger with NullHandler
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


sys.path.insert(0, str(Path(__file__).parent.parent.parent))

class StarkwareETHProxyCheck(ProxyCheck):
    async def implementation_address(self, address: Address, client: Client) -> Optional[int]:
        return await self.get_implementation(
            address=address,
            client=client,
            get_class_func=client.get_class_hash_at,
            regex_err_msg=r"(is not deployed)",
        )

    async def implementation_hash(self, address: Address, client: Client) -> Optional[int]:
        return await self.get_implementation(
            address=address,
            client=client,
            get_class_func=client.get_class_by_hash,
            regex_err_msg=r"(is not declared)",
        )

    @staticmethod
    async def get_implementation(
        address: Address, client: Client, get_class_func: Callable, regex_err_msg: str
    ) -> Optional[int]:
        call = StarkwareETHProxyCheck._get_implementation_call(address=address)
        err_msg = r"(Entry point 0x[0-9a-f]+ not found in contract)|" + regex_err_msg
        try:
            (implementation,) = await client.call_contract(call=call)
            await get_class_func(implementation)
        except ClientError as err:
            if re.search(err_msg, err.message, re.IGNORECASE) or err.code == RPC_CONTRACT_ERROR:
                return None
            raise err
        return implementation

    @staticmethod
    def _get_implementation_call(address: Address) -> Call:
        return Call(
            to_addr=address,
            selector=get_selector_from_name("implementation"),
            calldata=[],
        )


def get_proxy_config():
    return ProxyConfig(
        max_steps=5,
        proxy_checks=[StarkwareETHProxyCheck(), ArgentProxyCheck(), OpenZeppelinProxyCheck()],
    )

@dataclass
class DataConfig:
    """Configuration for Paradex data operations"""
    client: ParadexApiClient = ParadexApiClient(env=PROD)
    base_dir: Path = Path("data/paradex/chain")
    _full_node_client: FullNodeClient = None
    paradex_system_config: SystemConfig = None
    paraclear_contract: Contract = None
    usdc_contract: Contract = None
    oracle_contract: Contract = None
    contract_map: dict[int, Contract] = field(default_factory=dict)
    contract_names: dict[int, str] = field(default_factory=dict)

    def mkdirs(self) -> None:
        self.base_dir.mkdir(exist_ok=True, parents=True)

    def full_node_client(self) -> FullNodeClient:
        if self._full_node_client is None:
            logger.info("Creating full node client")
            config = self.client.fetch_system_config()
            self._full_node_client = FullNodeClient(
                node_url=config.starknet_fullnode_rpc_url,
            )
            self.paradex_system_config = config
        return self._full_node_client
    
    def load_contracts(self) -> None:
        paraclear_address = self.paradex_system_config.paraclear_address
        paraclear_contract = asyncio.run(Contract.from_address(address=paraclear_address, provider=self.full_node_client(), proxy_config=True))
        self.paraclear_contract = paraclear_contract

        usdc_address = self.paradex_system_config.bridged_tokens[0].l2_token_address
        usdc_contract = asyncio.run(Contract.from_address(address=usdc_address, provider=self.full_node_client(), proxy_config=get_proxy_config()))
        self.usdc_contract = usdc_contract

        oracle_address = self.paradex_system_config.oracle_address
        oracle_contract = asyncio.run(Contract.from_address(address=oracle_address, provider=self.full_node_client(), proxy_config=True))
        self.oracle_contract = oracle_contract
        self.contract_map[paraclear_contract.address] = paraclear_contract
        self.contract_map[usdc_contract.address] = usdc_contract
        self.contract_map[oracle_contract.address] = oracle_contract
        self.contract_names[paraclear_contract.address] = decode_shortstring(tuple(asyncio.run(paraclear_contract.functions["getName"].call()))[0])
        self.contract_names[usdc_contract.address] = decode_shortstring(tuple(asyncio.run(usdc_contract.functions["name"].call()))[0])
        self.contract_names[oracle_contract.address] = decode_shortstring(tuple(asyncio.run(oracle_contract.functions["getName"].call()))[0])
        print(self.contract_names)

def unpack(results: Dict) -> List:
    """Unpack results"""
    return results['results']

def block_timerange(data_config: DataConfig = DataConfig()) -> Tuple[int, int]:
    """Get block range from full node client"""
    latest_block = asyncio.run(data_config.full_node_client().get_block_number())
    logger.info(f"Latest block: {latest_block}")
    return [1, latest_block]

def process_block(data_config: DataConfig = DataConfig(), block_number: int = 1):
    """Process a single block"""
    event_data = defaultdict(lambda: defaultdict(list))
    
    block_events = asyncio.run(data_config.full_node_client().get_events(
        from_block_number=block_number, 
        to_block_number=block_number, 
        follow_continuation_token=True, 
        chunk_size=1000
    ))
    
    logger.info(f"Processing block {block_number} with {len(block_events.events)} events")
    for event in block_events.events:
        contract_address = event.from_address
        if contract_address not in data_config.contract_map:
            continue
            
        contract = data_config.contract_map[contract_address]
        contract_name = data_config.contract_names[contract_address]
        logger.debug(f"Processing event {event} for contract {contract_name}")
        
        event_name = None
        for name, abi in contract.data.parsed_abi.events.items():
            if get_selector_from_name(name) == event.keys[0]:
                event_name = name
                break
                
        if event_name is None:
            logger.error(f"Event name not found for key {event.keys[0]}")
            continue
            
        try:
            event_abi = contract.data.parsed_abi.events[event_name]
            event_serializer = serializer_for_event(event_abi)
            
            # Deserialize event data
            deserialized_data = event_serializer.deserialize(event.data).as_dict()
            
            # Try to get the predefined type from paraclear_types
            event_type = globals().get(event_abi.name)
            if event_type is None:
                logger.warning(f"No predefined type found for {event_abi.name}, using dynamic dataclass")
                fields = [(key, type(value)) for key, value in deserialized_data.items()]
                event_type = make_dataclass(event_abi.name, fields=fields)
            
            # Create instance of the class with the deserialized data
            event_instance = event_type(**deserialized_data)
            event_data[contract_name][event_abi.name].append(event_instance.to_dict())
            
        except (KeyError, Exception) as e:
            logger.error(f"Error processing event {event_name} for contract {contract_name}: {e}")
            continue

    # Save events to parquet files
    for contract_name, events in event_data.items():
        for event_name, data in events.items():
            print(event_name)
            if not data:
                continue
                
            event_dir = data_config.base_dir / "events" / contract_name / event_name
            event_dir.mkdir(parents=True, exist_ok=True)
            df = pd.DataFrame(data)
            output_file = event_dir / f"{block_number}.parquet"
            logger.info(f"Saving {len(df)} {event_name} events for {contract_name} to {output_file}")
            df.to_parquet(str(output_file))
            
            logger.info(f"Saved {len(df)} {event_name} events for {contract_name} to {output_file}")

def load_markets(data_config: DataConfig = DataConfig()):
    """Load markets data from Paradex API and save to parquet file"""
    markets = data_config.client.fetch_markets()
    df = pd.DataFrame(unpack(markets))
    
    # Create data directory if it doesn't exist
    data_config.mkdirs()
    
    df.to_parquet(str(data_config.base_dir / "markets.parquet"))
    logger.info(f"Saved {len(df)} markets to {data_config.base_dir / 'markets.parquet'}")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    config = DataConfig()
    load_markets(config)
    block_range = block_timerange(config)
    print(block_range)
    config.load_contracts()
    for block_number in range(block_range[-1]-100, block_range[-1]):
        process_block(config, block_number)
