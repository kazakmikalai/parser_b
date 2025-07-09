import logging

# Configure console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

# Configure file handler
file_handler = logging.FileHandler("parser.log")
file_handler.setLevel(logging.DEBUG) # Set to DEBUG for detailed logs
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Set root logger level to DEBUG to capture all messages
logger.addHandler(console_handler)
logger.addHandler(file_handler)
