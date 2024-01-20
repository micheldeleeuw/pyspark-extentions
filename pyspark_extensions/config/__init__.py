from configparser import ConfigParser
import os
import pathlib

config_path = pathlib.Path(__file__).parent.absolute().parent.absolute().parent.absolute() / "config.ini"
config = ConfigParser()
config.read(config_path)

environment = os.getenv('pyspark_extensions_config', default='default')
ENVARS = config[environment]
