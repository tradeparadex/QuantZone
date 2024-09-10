# Use the latest Miniconda3 image as the base
FROM continuumio/miniconda3:latest

# Set environment variables to avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Update system and install required packages
RUN apt-get update && \
    apt-get install -y \
        build-essential \
        curl \
        g++ \
        gcc \
        iputils-ping \
        libssl-dev \
        libusb-1.0 \
        pkg-config \
        python3-dev \
        build-essential \
        curl \
        libssl-dev \
        pkg-config \
    && \
    rm -rf /var/lib/apt/lists/*

# Create a working directory
WORKDIR /app

# Copy the requirements file and the script into the container
COPY /standalone/ /app/

RUN groupadd -g 1000 paradexbot && \
    useradd -m -s /bin/bash --uid 1000 -g 1000 paradexbot && \
    chown -R paradexbot:paradexbot /app

USER paradexbot:paradexbot

# Install Rust toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/home/paradexbot/.cargo/bin:${PATH}"

ENV PARADEX_L1_ADDRESS=0x2A45c5f9c1B08812934F95B670a1a73e48E01781 \
    PARADEX_ENVIRONMENT=testnet \
    KAFKA_BOOTSTRAP_SERVERS=b-1.paradextestnet.tsydp4.c3.kafka.ap-northeast-1.amazonaws.com:9092,b-2.paradextestnet.tsydp4.c3.kafka.ap-northeast-1.amazonaws.com:9092,b-3.paradextestnet.tsydp4.c3.kafka.ap-northeast-1.amazonaws.com:9092 \
    METRICS_STORAGE_TYPE=kafka,statsd \
    PARADEX_ID=1 \
    ALGO_PARAMS_MARKET=ETH-USD-PERP \
    ALGO_PARAMS_PRICE_SOURCES=binance_spot:ETH-USDT \
    ALGO_PARAMS_PRICE_EMA_SEC=22 \
    ALGO_PARAMS_BASIS_EMA_SEC=2880 \
    ALGO_PARAMS_FR_EMA_SEC=2880 \
    ALGO_PARAMS_PRICING_BASIS_FACTOR=0.42 \
    ALGO_PARAMS_PRICING_VOLATILITY_FACTOR=0.088 \
    ALGO_PARAMS_PRICING_FUNDING_RATE_FACTOR=0.58 \
    ALGO_PARAMS_VOL_WINDOW_SIZE=3000 \
    ALGO_PARAMS_POS_LEAN_BPS_PER_100K_USD=400.0 \
    ALGO_PARAMS_GLOBAL_POS_LEAN_BPS_PER_100K_USD=8.0 \
    ALGO_PARAMS_EMPTY_BOOK_PENALTY=0.05 \
    ALGO_PARAMS_MAX_MARKET_LATENCY_SEC=5 \
    ALGO_PARAMS_MAX_DATA_DELAY_SEC=888 \
    ALGO_PARAMS_ORDER_LEVEL_SPREAD=280 \
    ALGO_PARAMS_ORDER_LEVEL_AMOUNT_BPS=700 \
    ALGO_PARAMS_ORDER_AMOUNT_USD=800 \
    ALGO_PARAMS_BUY_LEVELS=4 \
    ALGO_PARAMS_SELL_LEVELS=4 \
    ALGO_PARAMS_ASK_SPREAD=0.028 \
    ALGO_PARAMS_BID_SPREAD=0.030 \
    ALGO_PARAMS_ORDER_LEVEL_SPREAD_LAMBDA=0.5 \
    ALGO_PARAMS_ORDER_SIZE_SPREAD_LAMBDA=0.5 \
    ALGO_PARAMS_TAKER_THRESHOLD_BPS=3.6 \
    ALGO_PARAMS_MAX_POSITION_USD=40000 \
    ALGO_PARAMS_MAX_LEVERAGE=2.5 \
    ALGO_PARAMS_MAX_MARGIN_RATIO=8 \
    ALGO_PARAMS_ORDER_REFRESH_TOLERANCE_PCT=0.02 \
    ALGO_PARAMS_MINIMUM_SPREAD=0 \
    ALGO_PARAMS_BULK_REQUESTS=1


# Create a new conda environment named 'pubenv' and install dependencies
RUN conda create --name pubenv python=3.12 && \
    conda run -n pubenv pip install --user --upgrade pip && \
    conda run -n pubenv pip install --user -r requirements.txt

# Activate the environment and run the script
# Note: `conda run` is used to execute commands within the conda environment
CMD ["conda", "run", "--no-capture-output", "-n", "pubenv", "python", "start.py"]
