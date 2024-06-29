from metaflow import FlowSpec, step, Parameter, retry
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
from collections import defaultdict
import datetime

class BasicETLFlow(FlowSpec):
    # Parameters for the flow
    price_limit = Parameter('price_limit', help="Price limit for data extraction")

    @step
    def start(self):
        print("Starting ETL flow...")
        self.next(self.extract_data)

    @step
    @retry(exceptions=[Exception], max_retries=3, delay_secs=5)
    def extract_data(self):
        print("Extracting data from PostgreSQL...")
        # PostgreSQL connection details
        db_host = 'localhost'
        db_port = '5432'
        db_database = 'db'
        db_username = 'postgres'
        db_password = 'password'

        engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}')

        query = text("SELECT id, name, host_id, host_name, neighbourhood_group, neighbourhood, "
                     "latitude, longitude, room_type, price, minimum_nights, number_of_reviews, "
                     "last_review, reviews_per_month, calculated_host_listings_count, availability_365 "
                     "FROM airbnb_nyc WHERE price < :price_limit")

        with engine.connect() as connection:
            results = connection.execute(query, {'price_limit': self.price_limit}).fetchall()

        # Transforming data and storing in self.transformed_data
        self.transformed_data = []
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
            self.transformed_data.append(transformed_row)

        self.next(self.transform_data)

    @step
    @retry(exceptions=[Exception], max_retries=3, delay_secs=5)
    def transform_data(self):
        print("Transforming data...")

        for row in self.transformed_data:
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

        neighborhood_prices = defaultdict(list)
        for row in self.transformed_data:
            neighborhood_prices[row['neighbourhood']].append(row['price'])

        average_prices = {}
        for neighborhood, prices in neighborhood_prices.items():
            average_prices[neighborhood] = sum(prices) / len(prices) if prices else 0

        for row in self.transformed_data:
            row['average_price_neighborhood'] = average_prices.get(row['neighbourhood'], 0)

        for row in self.transformed_data:
            if row['reviews_per_month'] is None:
                row['reviews_per_month'] = 0

        self.next(self.load_data)

    @step
    @retry(exceptions=[Exception], max_retries=3, delay_secs=5)
    def load_data(self):
        print("Loading transformed data into PostgreSQL...")

        # PostgreSQL connection details (same as extract_data)
        db_host = 'localhost'
        db_port = '5432'
        db_database = 'db'
        db_username = 'postgres'
        db_password = 'password'

        engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_database}')
        Session = sessionmaker(bind=engine)
        session = Session()

        Base = declarative_base()

        class TransformedAirbnb(Base):
            __tablename__ = 'transformed_airbnb_nyc'

            id = Column(Integer, primary_key=True)
            name = Column(String)
            host_id = Column(Integer)
            host_name = Column(String)
            neighbourhood_group = Column(String)
            neighbourhood = Column(String)
            latitude = Column(Float)
            longitude = Column(Float)
            room_type = Column(String)
            price = Column(Float)
            minimum_nights = Column(Integer)
            number_of_reviews = Column(Integer)
            review_date = Column(DateTime)
            review_time = Column(DateTime)
            reviews_per_month = Column(Float)
            calculated_host_listings_count = Column(Integer)
            availability_365 = Column(Integer)
            average_price_neighborhood = Column(Float)

        Base.metadata.create_all(engine)

        try:
            for row in self.transformed_data:
                transformed_row = TransformedAirbnb(
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
                session.add(transformed_row)
            session.commit()
            print(f"Successfully loaded {len(self.transformed_data)} rows into transformed_airbnb_nyc table.")
        except Exception as e:
            session.rollback()
            print(f"Error loading data into PostgreSQL: {e}")
        finally:
            session.close()

        self.next(self.end)

    @step
    def end(self):
        print("ETL process completed.")

if __name__ == '__main__':
    BasicETLFlow()
