# ETL Process Documentation

## Introduction

This document outlines the Extract, Transform, Load (ETL) process implemented for the Airbnb New York City dataset using Python, PostgreSQL, and Metaflow. The ETL process involves extracting raw data from a PostgreSQL database, transforming it to derive insights, and loading the transformed data back into a PostgreSQL database.

## ETL Process Steps

### Step 1: Data Ingestion and Storage

**Dataset Selection:**
- The chosen dataset is the Airbnb New York City dataset from Kaggle, containing detailed information about Airbnb listings in New York City.

**Database Setup:**
- PostgreSQL was selected for data storage due to its robustness and relational database capabilities.
- A database named `airbnb_db` was created to store the dataset.

**Data Loading:**
- The dataset was loaded into PostgreSQL using a Python script (`load_data.py`).
- SQLAlchemy was used to connect to the database and insert data efficiently.
- A table named `airbnb_nyc` was created with appropriate column definitions to store the Airbnb data.

### Step 2: ETL Process

**Data Extraction:**
- Data was extracted from the `airbnb_nyc` table using SQLAlchemy in Python (`extract_data.py`).
- A SQL query was executed to fetch data based on a specified `price_limit` parameter.

**Data Transformation:**
- Extracted data was transformed to enhance its usability and analytics capabilities.
- Transformation steps included:
  - Normalizing the `last_review` column into separate `review_date` and `review_time` columns.
  - Calculating additional metrics such as average price per neighborhood.
  - Handling missing values by filling them with appropriate default values.

**Data Loading:**
- Transformed data was loaded into a new table named `transformed_airbnb_nyc` using SQLAlchemy (`load_transformed_data.py`).
- The table schema was designed to accommodate the transformed data structure, ensuring data integrity and efficiency.

### Step 3: Workflow Management with Metaflow

**Workflow Implementation:**
- Metaflow, a Python framework for managing data science workflows, was used to orchestrate the ETL process.
- The workflow (`airbnb_etl_flow.py`) was defined with multiple steps:
  - `start`: Initiates the ETL process.
  - `extract_data`: Extracts data from PostgreSQL based on a parameterized price limit.
  - `transform_data`: Transforms the extracted data by normalizing columns and calculating metrics.
  - `load_data`: Loads the transformed data back into PostgreSQL.

**Error Handling and Retry Mechanism:**
- Retry mechanisms were implemented using Metaflow’s `retry` decorator to handle exceptions gracefully and ensure workflow robustness.
- Maximum retries and delay intervals were specified to manage transient errors during data extraction, transformation, or loading.

## Instructions for Running the Metaflow Workflow

### Prerequisites

**Software Requirements:**
- Python (version 3.6 or higher)
- PostgreSQL database server
- Metaflow (install via `pip install metaflow`)

**Database Setup:**
- Ensure PostgreSQL is installed and running.
- Create a database named `airbnb_db` to store the Airbnb dataset.

### Running the Metaflow Workflow

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/Dibyendu-13/AI_Planet_Assignment.git
   cd AI_Planet_Assignment
Configure PostgreSQL Credentials:

Update database connection details (db_host, db_port, db_name, db_user, db_password) in the Metaflow workflow script (airbnb_etl_flow.py).
Execute the Metaflow Workflow.

Run the Metaflow workflow using Python:

python etl_flow.py run
Follow the prompts to provide the price_limit parameter for data extraction.

Metaflow provides real-time logs and status updates during workflow execution.
Errors or retries will be logged with detailed messages to aid in troubleshooting.

Check PostgreSQL tables (airbnb_nyc, transformed_airbnb_nyc) to ensure data has been loaded and transformed correctly.

# Run the Metaflow workflow with a price limit of 200
python airbnb_etl_flow.py run --price_limit 200

This documentation provides a comprehensive overview of the ETL process implemented for the Airbnb New York City dataset using Metaflow and PostgreSQL. It includes step-by-step instructions for setting up the environment, running the workflow, and verifying the results. Adjust the specifics (like database credentials and file names) to match your actual implementation.
