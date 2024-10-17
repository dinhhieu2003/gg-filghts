import configparser
import sys
import os

root_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, root_dir)

config_directory = os.path.join(root_dir, 'config/config.conf')

parser = configparser.ConfigParser()
parser.read(config_directory)

FLIGHTS_API_KEY = parser.get(section='api_keys', option='flights_api_key')