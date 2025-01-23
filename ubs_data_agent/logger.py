import logging
import sys

def get_logger(name):
    """
    Returns a logger with the specified name.
    This logger can be imported and used across multiple modules.
    """
    # Create or retrieve a logger
    logger = logging.getLogger(name)
    
    # Avoid adding duplicate handlers if the logger already exists
    if logger.hasHandlers():
        return logger

    # Set the minimum logging level
    logger.setLevel(logging.DEBUG)

    # Create a console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)  # Change level if needed

    # Create a formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] (%(name)s): %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(console_handler)

    return logger
