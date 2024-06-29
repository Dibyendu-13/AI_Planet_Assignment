
## Setup Instructions

### Installation

1. **Python and Dependencies:**
   - Ensure Python 3.7+ is installed.
   - Install dependencies using pip:
     ```
     pip install -r requirements.txt
     ```

2. **Database Setup:**
   - Install PostgreSQL and create a database named `db`.
   - Set up your database credentials in `etl_flow.py`.

## Running the ETL Workflow

### Metaflow Setup

1. **Install Metaflow:**
   - Metaflow is already included in `requirements.txt`. Install it with:
     ```
     pip install -r requirements.txt
     ```

2. **Running the Workflow:**
   - Execute the Metaflow workflow using the following command:
     ```
     python etl_flow.py run
     ```

## Documentation

For detailed documentation, refer to:

- [ETL Process](docs/ETL_Process.md): Explanation of the ETL process and data transformations.
- [Metaflow Workflow](docs/Metaflow.md): Instructions for setting up and running the Metaflow workflow.

## Contributing

Feel free to contribute to this project. Please fork the repository, make your changes, and submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
