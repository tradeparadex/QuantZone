from typing import Dict, List, Tuple, Callable, Any
import duckdb
import pandas as pd
from paradex_py.api.api_client import ParadexApiClient
from paradex_py.api.models import SystemConfig
from paradex_py.environment import PROD
from starknet_py.net.full_node_client import FullNodeClient
from starknet_py.contract import Contract
from starknet_py.net.client_models import TransactionTrace, InvokeTransactionTrace
from pathlib import Path
from dataclasses import dataclass
import logging
import asyncio

# Create module logger with NullHandler
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

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
    contract_map: Dict[int, Contract] = {}

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
        usdc_contract = asyncio.run(Contract.from_address(address=usdc_address, provider=self.full_node_client(), proxy_config=False))
        self.usdc_contract = usdc_contract

        oracle_address = self.paradex_system_config.oracle_address
        oracle_contract = asyncio.run(Contract.from_address(address=oracle_address, provider=self.full_node_client(), proxy_config=True))
        self.oracle_contract = oracle_contract
        self.contract_map[paraclear_contract.address] = paraclear_contract
        self.contract_map[usdc_contract.address] = usdc_contract
        self.contract_map[oracle_contract.address] = oracle_contract

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
    # blockdata = asyncio.run(data_config.full_node_client().trace_block_transactions(block_number))
    # block_events = asyncio.run(data_config.full_node_client().get_events(block_number))


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
    process_block(config, block_range[-1])