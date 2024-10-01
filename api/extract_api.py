import requests
import pandas as pd

# URL del API del CDC
url = 'https://data.cdc.gov/resource/bi63-dtpu.json'

# solicitud request
response = requests.get(url)
data = response.json()

# convertimos to DataFrame
df = pd.DataFrame(data)

# guardamos los datos en un csv
df.to_csv('cdc_raw_data.csv', index=False)
