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

ENV ALGO_PARAMS_MINIMUM_SPREAD=0 \
    ALGO_PARAMS_BULK_REQUESTS=1


# Create a new conda environment named 'pubenv' and install dependencies
RUN conda create --name pubenv python=3.12 && \
    conda run -n pubenv pip install --user --upgrade pip && \
    conda run -n pubenv pip install --user -r requirements.txt

# Activate the environment and run the script
# Note: `conda run` is used to execute commands within the conda environment
CMD ["conda", "run", "--no-capture-output", "-n", "pubenv", "python", "start.py"]
