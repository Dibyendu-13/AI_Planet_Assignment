from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, Time

# PostgreSQL database connection credentials
db_host = 'localhost'
db_port = '5432'
db_database = 'db'
db_username = 'postgres'
db_password = 'password'

# Create the PostgreSQL engine
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}')

# Reflect the existing table schema for reference
metadata = MetaData()
metadata.reflect(bind=engine)

# Define the new table schema
new_table_name = 'transformed_airbnb_nyc'
new_table = Table(new_table_name, metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('host_id', Integer),
    Column('host_name', String),
    Column('neighbourhood_group', String),
    Column('neighbourhood', String),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('room_type', String),
    Column('price', Float),
    Column('minimum_nights', Integer),
    Column('number_of_reviews', Integer),
    Column('reviews_per_month', Float),
    Column('calculated_host_listings_count', Integer),
    Column('availability_365', Integer),
    Column('review_date', Date),
    Column('review_time', Time),
)

metadata.create_all(engine)  # Create the new table in the database
