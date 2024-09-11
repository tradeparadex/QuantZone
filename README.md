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

### How to Set Parameters

Parameters can be passed as environment variables. Here are examples for different setups:

#### Docker

In Dockerfile:
```
ENV PARAM_NAME=value
```

When running the container:
```
docker run -e PARAM_NAME=value paradex-market-maker
```

#### Anaconda

Before running the script:
```
export PARAM_NAME=value
```
### Available Parameters

- `PARAM_CLOSE_ONLY_MODE`: Enable close-only mode
- `PARAM_ENABLED`: Enable/disable the strategy
- `PARAM_ORDER_LEVEL_SPREAD`: Spread between order levels
- `PARAM_ORDER_LEVEL_AMOUNT_PCT`: Percentage of order amount per level
- `PARAM_ORDER_REFRESH_TIME_SEC`: Time interval for refreshing orders
- `PARAM_ORDER_REFRESH_TOLERANCE_PCT`: Tolerance for order refreshing
- `PARAM_BUY_LEVELS`: Number of buy levels
- `PARAM_SELL_LEVELS`: Number of sell levels
- `PARAM_BID_SPREAD`: Spread for bid orders
- `PARAM_ASK_SPREAD`: Spread for ask orders
- `PARAM_MINIMUM_SPREAD`: Minimum allowed spread
- `PARAM_ORDER_AMOUNT_USD`: Order amount in USD
- `PARAM_POS_LEAN_BPS_PER_100K_USD`: Position leaning in basis points per 100K USD
- `PARAM_MAX_POSITION_USD`: Maximum position size in USD
- `PARAM_TAKER_THRESHOLD_BPS`: Threshold for taker orders in basis points
- `PARAM_PRICE_EMA_SEC`: EMA period for price in seconds
- `PARAM_FR_EMA_SEC`: EMA period for funding rate in seconds
- `PARAM_BASIS_EMA_SEC`: EMA period for basis in seconds
- `PARAM_MAX_LEVERAGE`: Maximum allowed leverage
- `PARAM_MAX_MARGIN_RATIO`: Maximum margin ratio
- `PARAM_GLOBAL_POS_LEAN_BPS_PER_100K_USD`: Global position leaning in basis points per 100K USD
- `PARAM_PRICE_CEILING`: Upper price limit
- `PARAM_PRICE_FLOOR`: Lower price limit

## How to Build

To customize the strategy:

1. Override the `get_fair_price` method to implement your own pricing logic. This allows you to quote around any pricing model you prefer.
2. The risk manager module can be overridden to implement custom risk management strategies.

We encourage users to create Pull Requests with their improvements and customizations. This collaborative approach helps in evolving the strategy and benefits the entire community.

## License

This project is released under the MIT License. It is provided for educational purposes only. The authors and contributors bear no responsibility for any financial losses or other damages incurred through the use of this software.












