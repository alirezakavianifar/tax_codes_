import time
import logging
import sys

# Configure logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('script_example.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

try:
    logging.info('started script hello_world')
    time.sleep(2)
    logging.info('hello world2')
    time.sleep(5)
    logging.info('done hello_world')
except Exception as e:
    logging.error(f"Script failed: {e}", exc_info=True)
