import pandas as pd
from sqlalchemy import create_engine

# Replace with your actual database credentials
db_host = 'localhost'
db_port = '5432'
db_database = 'db'
db_username = 'postgres'
db_password = 'password'

# Create the PostgreSQL engine
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}')

# Load CSV into Pandas DataFrame
df = pd.read_csv('AB_NYC_2019.csv')

# Insert data into PostgreSQL
df.to_sql('airbnb_nyc', engine, if_exists='replace', index=False)
