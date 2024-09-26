# QuantZone

## Market Making Strategy for Paradex Perpetuals

### Introduction

This algorithm implements a market making strategy for perpetual instruments on the Paradex exchange. It listens to an external exchange (Binance) for spot prices and quotes around that value, with adjustments for:

- Perpetual basis
- Funding rate
- Volatility
- Account positions

The strategy is designed to be simple, always quoting both sides of the market and aiming to capture the spread when the market is flat.

We welcome contributions, improvements, questions, and suggestions from the community. Feel free to build upon this strategy, enhance its capabilities, or propose new features. This project is intended for educational purposes and to foster collaboration within the algorithmic trading community.

### How to Run

#### Option 1: Using Docker

1. Ensure Docker is installed on your system.
2. Navigate to the repository root.
3. Build the Docker image:
```
docker build -t paradex-market-maker .
```

### Option 2: Using Anaconda

1. Ensure Anaconda is installed on your system.
2. Create a new conda environment:
```
conda create -n paradex-mm python=3.14
conda activate paradex-mm
```
3. Install the requirements:
```
pip install -r requirements.txt
```
4. Run the strategy:
```
python start.py
```

### Authentication

To authenticate with Paradex, you need to provide your Wallet address and private key. You can get more information in the [Paradex API documentation](https://docs.paradex.trade/developer-portal/general-information/api-quick-start).

You can then set the `PARADEX_L1_ADDRESS` and `PARADEX_PRIVATE_KEY` environment variables. See below for more details. You will also need to set the `PARADEX_ENVIRONMENT` environment variable to `mainnet` or `testnet`.

Example:
```
export PARADEX_L1_ADDRESS="0x1234567890123456789012345678901234567890"
export PARADEX_PRIVATE_KEY="0x1234567890123456789012345678901234567890123456789012345678901234"
export PARADEX_ENVIRONMENT="testnet"
```

### How to Set Parameters

Parameters can be passed as environment variables OR a YAML configuration file. Here are examples for different setups:

#### Docker

In Dockerfile:
```
ENV PARAM_NAME=value
```

When running the container:
```
docker run -e PARAM_NAME=value paradex-market-maker
```

If using a YAML configuration file:
Edit `strategy_settings.yaml`:

```
parameters:
  PARAM_NAME: value
```

#### Anaconda

Before running the script:
```
export PARAM_NAME=value
```
### Available Parameters

- `PARAM_CLOSE_ONLY_MODE`: Enable close-only mode - when enabled the strategy will only quote on the side that reduces the positon.
- `PARAM_ENABLED`: Enable/disable the strategy
- `PARAM_ORDER_LEVEL_SPREAD`: Spread between order levels (in tick size terms)
- `PARAM_ORDER_LEVEL_AMOUNT_PCT`: Percentage of order amount increase per level quoted
- `ALGO_PARAMS_ORDER_INSERT_TIME_SEC`: Minimum time interval for refreshing orders (in seconds)
- `PARAM_ORDER_REFRESH_TOLERANCE_PCT`: Tolerance for order refreshing - a new order will only be inserted if the difference between the current live order and the intended price is bigger than the tolerance threshold
- `PARAM_BUY_LEVELS`: Number of buy levels
- `PARAM_SELL_LEVELS`: Number of sell levels
- `PARAM_BID_SPREAD`: Spread for bid orders (in bps)
- `PARAM_ASK_SPREAD`: Spread for ask orders (in bps)
- `PARAM_MINIMUM_SPREAD`: Minimum allowed spread
- `PARAM_ORDER_AMOUNT_USD`: Order amount in USD
- `PARAM_POS_LEAN_BPS_PER_100K_USD`: Position leaning in bps per $100K in the instrument. It influences how the algorithm adjusts its bid and ask prices to counterbalance existing positions. For instance, if the market maker holds a large long position, increasing this parameter would cause the algorithm to set lower ask prices to encourage selling, helping to reduce the long exposure.
- `PARAM_GLOBAL_POS_LEAN_BPS_PER_100K_USD`: Global position leaning in bps per $100K in the account
- `PARAM_MAX_POSITION_USD`: Maximum position size in USD
- `PARAM_TAKER_THRESHOLD_BPS`: Threshold for taker orders in basis points
- `PARAM_PRICE_EMA_SEC`: EMA period for price in seconds. A shorter half-life increases the EMA's responsiveness to recent market changes. A longer half-life reduces reactivity to minor fluctuations.
- `PARAM_FR_EMA_SEC`: EMA period for funding rate in seconds
- `PARAM_BASIS_EMA_SEC`: EMA period for basis in seconds
- `PARAM_MAX_LEVERAGE`: Maximum allowed leverage
- `PARAM_MAX_MARGIN_RATIO`: Maximum margin ratio
- `PARAM_PRICE_CEILING`: Upper price limit
- `PARAM_PRICE_FLOOR`: Lower price limit
- `ALGO_PARAMS_PRICE_SOURCES`: External markets to use for pricing. Format is `EXCHANGE:INSTRUMENT`, for example `binance_spot:BTCUSDT`. Leave empty to use the instrument price only.

## How to Build

To customize the strategy:

1. Override the `get_fair_price` method to implement your own pricing logic. This allows you to quote around any pricing model you prefer.
2. The risk manager module can be overridden to implement custom risk management strategies.

We encourage users to create Pull Requests with their improvements and customizations. This collaborative approach helps in evolving the strategy and benefits the entire community.

## License

This project is released under the MIT License. It is provided for educational purposes only. The authors and contributors bear no responsibility for any financial losses or other damages incurred through the use of this software.












