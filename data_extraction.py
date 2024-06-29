from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, text, func
from collections import defaultdict
import datetime

# PostgreSQL database connection credentials
db_host = 'localhost'
db_port = '5432'
db_database = 'db'
db_username = 'postgres'
db_password = 'password'

# Create the PostgreSQL engine
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}')

# Reflect the table schema of airbnb_nyc
metadata = MetaData()
metadata.reflect(bind=engine)

# Define the table structure for transformed_airbnb_nyc
transformed_table = Table(
    'transformed_airbnb_nyc',
    metadata,
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
    Column('review_date', DateTime),
    Column('review_time', DateTime),
    Column('reviews_per_month', Float),
    Column('calculated_host_listings_count', Integer),
    Column('availability_365', Integer),
    Column('average_price_neighborhood', Float)
)

# Commit table creation to database
metadata.create_all(engine)

# Using textual SQL query to fetch and transform data
query = text("SELECT id, name, host_id, host_name, neighbourhood_group, neighbourhood, "
             "latitude, longitude, room_type, price, minimum_nights, number_of_reviews, "
             "last_review, reviews_per_month, calculated_host_listings_count, availability_365 "
             "FROM airbnb_nyc WHERE price < :price_limit")

# Execute the query and fetch all results
with engine.connect() as connection:
    results = connection.execute(query, {'price_limit': 100}).fetchall()

# Transform the results into a list of dictionaries
transformed_data = []
for row in results:
    transformed_row = {
        'id': row[0],
        'name': row[1],
        'host_id': row[2],
        'host_name': row[3],
        'neighbourhood_group': row[4],
        'neighbourhood': row[5],
        'latitude': row[6],
        'longitude': row[7],
        'room_type': row[8],
        'price': row[9],
        'minimum_nights': row[10],
        'number_of_reviews': row[11],
        'last_review': row[12],
        'reviews_per_month': row[13],
        'calculated_host_listings_count': row[14],
        'availability_365': row[15],
    }
    transformed_data.append(transformed_row)

# Iterate through transformed_data to handle datetime and missing values
for row in transformed_data:
    if row['last_review']:
        try:
            last_review_datetime = datetime.datetime.strptime(row['last_review'], '%Y-%m-%d %H:%M:%S')
            row['review_date'] = last_review_datetime.date()
            row['review_time'] = last_review_datetime.time()
        except ValueError:
            try:
                last_review_date = datetime.datetime.strptime(row['last_review'], '%Y-%m-%d')
                row['review_date'] = last_review_date.date()
                row['review_time'] = None
            except ValueError:
                row['review_date'] = None
                row['review_time'] = None
    else:
        row['review_date'] = None
        row['review_time'] = None

    del row['last_review']

# Calculate additional metrics (average price per neighborhood)
neighborhood_prices = defaultdict(list)
for row in transformed_data:
    neighborhood_prices[row['neighbourhood']].append(row['price'])

average_prices = {}
for neighborhood, prices in neighborhood_prices.items():
    average_prices[neighborhood] = sum(prices) / len(prices) if prices else 0

for row in transformed_data:
    row['average_price_neighborhood'] = average_prices.get(row['neighbourhood'], 0)

for row in transformed_data:
    if row['reviews_per_month'] is None:
        row['reviews_per_month'] = 0

# Insert all rows of transformed data into the new table
with engine.connect() as connection:
    try:
        # Begin a transaction
        with connection.begin():
            # Execute insert queries for all rows
            for row in transformed_data:
                insert_query = transformed_table.insert().values(
                    id=row['id'],
                    name=row['name'],
                    host_id=row['host_id'],
                    host_name=row['host_name'],
                    neighbourhood_group=row['neighbourhood_group'],
                    neighbourhood=row['neighbourhood'],
                    latitude=row['latitude'],
                    longitude=row['longitude'],
                    room_type=row['room_type'],
                    price=row['price'],
                    minimum_nights=row['minimum_nights'],
                    number_of_reviews=row['number_of_reviews'],
                    review_date=row['review_date'],
                    review_time=row['review_time'],
                    reviews_per_month=row['reviews_per_month'],
                    calculated_host_listings_count=row['calculated_host_listings_count'],
                    availability_365=row['availability_365'],
                    average_price_neighborhood=row['average_price_neighborhood']
                )
                connection.execute(insert_query)

        # Query the number of rows inserted
        num_rows_inserted = connection.execute(
            func.count().select().where(transformed_table.c.id.in_([row['id'] for row in transformed_data]))
        ).scalar()

        print(f"Successfully inserted {num_rows_inserted} rows into transformed_airbnb_nyc table.")

    except Exception as e:
        print(f"Error inserting data: {e}")
