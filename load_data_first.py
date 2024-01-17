# Library use for the project

from psnawp_api import PSNAWP
import os
import pandas as pd


# Create the connection with the psn API using the psnawp_api library

psnawp = PSNAWP(os.getenv('psn'))

client = psnawp.me()

print(client)