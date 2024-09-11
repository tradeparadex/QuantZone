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
```docker build -t paradex-market-maker .```
