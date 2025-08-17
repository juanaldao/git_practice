# Imports
from datetime import datetime, timedelta
import os
import pytz
import pandas as pd
from ppi_client.ppi import PPI
from google.oauth2 import service_account
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Set Google Application Credentials
key_path = "/app/ppi-trading-bot-sa.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Authenticate using service account key
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project="ppi-trading-bot")

# Load credentials from environment variables
PPI_USERNAME = os.getenv("public_key")
PPI_PASSWORD = os.getenv("private_key")
if not PPI_USERNAME or not PPI_PASSWORD:
    raise EnvironmentError("Environment variables not loaded properly.")

PROJECT_ID = "ppi-trading-bot"
DATASET_TABLE = "operaciones.market_data"

# Initialize PPI API
ppi = PPI(sandbox=False)
ppi.account.login_api(PPI_USERNAME, PPI_PASSWORD)

# Define Argentina's timezone
argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")

# Get today's date and the date 320 days earlier
today = datetime.now(argentina_tz).date()
earlier_date = today - timedelta(days=320)

# Fetch market data
ticker = "SPYD"
tipo_instrumento = "Cedears"
market_data = ppi.marketdata.search(ticker, tipo_instrumento, "A-24HS", date_from=earlier_date, date_to=today)

# Convert market data to DataFrame
def get_dataframe_from_marketdata(marketdata):
    return pd.DataFrame.from_dict(marketdata)

df_marketdata = get_dataframe_from_marketdata(market_data)

# Drop unnecessary columns
drop_columns = ["previousClose", "marketChange", "marketChangePercent"]
df_marketdata.drop(columns=drop_columns, errors="ignore", inplace=True)

# Convert "date" column to date
df_marketdata["date"] = pd.to_datetime(df_marketdata["date"]).dt.date

# Calculate moving averages
medias = [5, 200]
for media in medias:
    df_marketdata[f"sma{media}"] = df_marketdata["price"].rolling(media).mean()

# Condition (i): Upward movement (price > sma200)
df_marketdata["movimiento_alcista"] = df_marketdata["price"] > df_marketdata["sma200"]

# Condition (ii): Tres velas rojas (openingPrice > price)
df_marketdata["red_candle"] = df_marketdata["openingPrice"] > df_marketdata["price"]
df_marketdata["tres_velas_rojas"] = df_marketdata["red_candle"].rolling(window=3).apply(lambda x: x.all(), raw=True).fillna(0).astype(bool)
df_marketdata.drop(columns=["red_candle"], inplace=True)

# Condition (iii): Closing price is lower than the previous one for three consecutive days. No necesariamente son three red candles.
df_marketdata["price_diff"] = df_marketdata["price"].diff()
df_marketdata["tres_velas_descendientes"] = (
    (df_marketdata["price_diff"] < 0) &
    (df_marketdata["price_diff"].shift(1) < 0) &
    (df_marketdata["price_diff"].shift(2) < 0)
)
df_marketdata.drop(columns=["price_diff"], inplace=True)

# Crea columna boolean para compra
df_marketdata["compra"] = (
    df_marketdata["movimiento_alcista"] &
    df_marketdata["tres_velas_rojas"] &
    df_marketdata["tres_velas_descendientes"]
)

# Crea columna boolean para venta
df_marketdata["venta"] = df_marketdata["price"] > df_marketdata["sma5"]

daily_register = df_marketdata.tail(1)

# BigQuery setup
client = bigquery.Client(project=PROJECT_ID)

# Fetch max date from BigQuery
query = f"""
SELECT MAX(date) AS max_date
FROM `{PROJECT_ID}.{DATASET_TABLE}`
"""
max_date_df = client.query(query).to_dataframe()
max_date = max_date_df['max_date'].iloc[0] if not max_date_df.empty else None

daily_date = daily_register['date'].iloc[0]

# Insert new data if applicable
if max_date is None or daily_date > max_date:
    to_gbq(daily_register, DATASET_TABLE, project_id=PROJECT_ID, if_exists="append")
    print("New data inserted successfully.")
else:
    print("Data already up to date. No insertion needed.")