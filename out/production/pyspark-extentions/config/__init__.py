import logging
from configparser import ConfigParser
import os
import pathlib

log = logging.getLogger(__name__)

config_path = pathlib.Path(__file__).parent.absolute().parent.absolute() / "config.ini"
log.debug(f'config.ini path: {config_path}')
config = ConfigParser()
config.read(config_path)

environment = os.getenv('pyspark_extensions_config', default='default')
ENVARS = config[environment]
