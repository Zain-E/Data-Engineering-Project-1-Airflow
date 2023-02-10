# Press ⌃R to execute it or replace it with your code.
# Press shift, option, E to execute highlighted code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


#------------------------------------     UPDATE (GBQ CONNECT)     -----------------------------------------
#-----------------------------------------------------------------------------------------------------------

import pandas as pd
import pandas_gbq as pd_gbq
from google.cloud import bigquery
from google.oauth2 import service_account

# Set up your GCP project and service account credentials (downloaded as a JSON from 'service accounts' on GCP)
credentials = service_account.Credentials.from_service_account_file('zains-gcp-compute_engine.json')
project_id = 'zains-gcp'

# Connect to the right project id and client
client = bigquery.Client(credentials=credentials,project=project_id)

# Create a df from the tables within GBQ
dataset_name = 'zains-gcp.Datatonic.daily_player_summary_v2'

query = """
SELECT * 
FROM `zains-gcp.Datatonic.daily_player_summary_v2` 
LIMIT 1000
"""
df = pd_gbq.read_gbq(f'{dataset_name}', project_id=f'{project_id}')

print(df.shape())