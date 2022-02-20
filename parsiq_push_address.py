from dotenv import load_dotenv
import os
import etl_utls as utl
import requests

load_dotenv(".env")
PARSIQ_API_KEY = os.environ.get("PARSIQ_API_KEY")
PARSIQ_TABLE_ID = os.environ.get("PARSIQ_TABLE_ID")

data = utl.query_postgres(sql="select id as address from insider;", columns=["address"])
data_raw = data.to_json(orient="records")
url = f"https://api.parsiq.net/v1/data/{PARSIQ_TABLE_ID}/"
headers = {"Authorization": f"Bearer {PARSIQ_API_KEY}", "Content-Type": "application/json"}
requests.put(url=url, headers=headers, data=data_raw, allow_redirects=True)
