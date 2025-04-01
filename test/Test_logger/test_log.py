from logger.loggers import get_logger

# Get the logger
logger = get_logger('PRODUCT_MANAGER')

def test_logging():

    # Log messages at different levels
    logger.debug("This is a DEBUG message - should appear in the file.")
    logger.info("This is an INFO message - should appear in both console and file.")
    logger.warning("This is a WARNING message - should appear in both console and file.")
    logger.error("This is an ERROR message - should appear in both console and file.")
    logger.critical("This is a CRITICAL message - should appear in both console and file.")

    # Force log handlers to flush
    for handler in logger.handlers:
        handler.flush()

    print("✅ Logging test completed. Check console and log file for entries.")

# Call the function to execute the test
if __name__ == "__main__":
    test_logging()
