import asyncio
import logging
import os
# Configure the logging
logging.basicConfig(
    level=os.environ.get('LOG_LEVEL', 'INFO'),  # Set the logging level
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the log message format
    datefmt='%Y-%m-%d %H:%M:%S',  # Set the date and time format
    # filename='app.log',  # Log messages to a file
    # filemode='a'  # Append to the log file
)
import signal
import traceback
import sys

from strategy import PerpMarketMaker


async def shutdown(signal, loop, my_process):
    my_process.logger.info(f"Received exit signal {signal.name}...")
    my_process.graceful_shutdown()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    my_process.logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

def handle_exception(loop, context):
    # This function will be called for exceptions that are not handled in tasks
    msg = context.get("exception", context["message"])
    logging.error(f"Caught exception: {msg}")


async def main():

    loop=asyncio.get_running_loop()
    strategy = PerpMarketMaker(loop=loop)

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

