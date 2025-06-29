# %%
# Expoloring the root logger

import logging

logging.debug("This is a debug message")
logging.info("This is an info message")
logging.warning("This is a warning message")
logging.error("This is an error message")
logging.critical("This is a critical message")


# %%
# Adjusting the log level

import logging

logging.basicConfig(level=logging.DEBUG) # sets up a default handler (if none exists)
# Alternate
logging.root.handlers = [] # clear existing handlers 
logging.getLogger().setLevel(logging.DEBUG) # manually forces the debug level
logging.debug("Debug message")

# %%
# Formatting the output

import logging

logging.root.handlers = []

# logging.basicConfig(format="%(levelname)s:%(message)s")
# alternate to format
logging.basicConfig(
    format="{asctime}::{levelname}::{message}", 
    style="{",
    datefmt="%Y-%m-%d %H-%M"
)
logging.warning('Warning messsage')

# %%
# Logging to a file

import logging

logging.basicConfig(
    filename='app.log',
    encoding='utf-8',
    filemode='a',
    format="{asctime} : {levelname} : {message}",
    style="{",
    datefmt="%Y-%m-%d %H-%M"
)
logging.warning('Saveme!')

# %%
# Displaying variable data

import logging

logging.basicConfig(
    format="{asctime} {levelname} {message}",
    style="{"
)
name='Rohit'
logging.debug(f"{name=}")

# %%
# Capturing Stack Traces

import logging 
logging.basicConfig(
    filename='app.log',
    encoding='utf-8',
    filemode='a',
    format="{asctime} : {levelname} : {message}",
    style="{",
    datefmt="%Y-%m-%d %H-%M"
)
donuts = 5
guests = 0
# try:
#     donuts_per_guests = donuts/guests 
# except ZeroDivisionError:
#     logging.error("DonutCalcualtionError", exc_info=True)
# Alternative method
try:
    donuts_per_guest = donuts/guests
except ZeroDivisionError:
    logging.exception("DonutCalcualtionError")

# %%
# Creating a Custom logger

# Instantiating your logger
import logging

logger = logging.getLogger(__name__)
logger.warning("New Logger")

# %%
# Using Handlers

import logging 

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler('app.log', mode='a', encoding='utf-8')
logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.handlers
logger.warning('Warned you!')

# %%
# Adding Formatters to your Handlers

import logging

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler('app.log',mode='a',encoding='utf-8')
logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.warning('Warned you again!!')
formatter = logging.Formatter(
    "{asctime} : {levelname} : {message}",
    style="{",
    datefmt="%Y %M %d"
)
console_handler.setFormatter(formatter)
logger.warning("NO WORRIES, formatted!!")

# %%
# Setting the Log Levels of Custom Loggers

import logging

logger = logging.getLogger(__name__)
logger.parent
logger.setLevel("DEBUG")
formatter = logging.Formatter("{levelname} : {message}", style="{")

console_handler = logging.StreamHandler()
console_handler.setLevel("DEBUG")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler('app.log', mode='a',encoding='utf-8')
file_handler.setLevel("WARNING")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.debug("Nedd to debug!!")
logger.warning("Warning Time")
logger.error("Error is there!")
