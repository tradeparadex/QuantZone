"""
Main entry point for the market making strategy.

This module sets up logging, imports necessary dependencies,
and initializes the perpetual futures market making strategy.
It also handles graceful shutdown and exception handling.

This is the place where you can define the overrides for the strategy; 
pricer and the risk modules.
"""
import logging
import structlog
import argparse
import asyncio
import os
import signal
import traceback
import sys
from strategy import PerpMarketMaker
from pricer_perps import PerpPricer

# Configure structlog
def configure_logging():
    logging_level = os.environ.get('LOG_LEVEL', 'INFO').upper()

    # Convert string level name to numeric value
    numeric_level = getattr(logging, logging_level, logging.INFO)

    # Define processors for structlog
    processors = [
        structlog.processors.CallsiteParameterAdder(
            [
                structlog.processors.CallsiteParameter.PATHNAME,
                structlog.processors.CallsiteParameter.LINENO,
                structlog.processors.CallsiteParameter.FUNC_NAME,
            ]
        ),
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt='%Y-%m-%d %H:%M:%S'),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer(),  # Use ConsoleRenderer for pretty output
    ]

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure root logger
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
    )

configure_logging()


parser = argparse.ArgumentParser()
parser.add_argument('--config', default='strategy_settings.yaml')
args = parser.parse_args()


async def shutdown(signal: signal.Signals, loop: asyncio.AbstractEventLoop, my_process: PerpMarketMaker) -> None:
    """
    Shutdown the strategy gracefully.
    """
    my_process.logger.info(f"Received exit signal {signal.name}...")
    my_process.graceful_shutdown()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    my_process.logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

def handle_exception(loop: asyncio.AbstractEventLoop, context: dict) -> None:
    """
    Handle exceptions that are not handled in tasks.
    """
    msg = context.get("exception", context["message"])
    structlog.get_logger().error("Caught exception", exception=msg)


async def main():

    loop=asyncio.get_running_loop()
    strategy = PerpMarketMaker(
        loop=loop, 
        PricerClass=PerpPricer,
        config_path=args.config
    )

    # Set up signal handlers
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop, strategy))
        )

    # Set up exception handler
    loop.set_exception_handler(handle_exception)

    try:
        # Initialize and run your strategy
        await strategy.run()
    except Exception as e:
        strategy.logger.error(f"Error running strategy: {e}")
        strategy.logger.error(traceback.format_exc())
    finally:
        try:
            strategy.stop()
        except Exception as stop_error:
            strategy.logger.error(f"Error stopping strategy: {stop_error}")
        return 1 

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

