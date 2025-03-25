import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Step 1: Extract - Load data from a CSV file
def extract(csv_file):
    df = pd.read_csv(csv_file)
    return df

# Step 2: Transform - Clean and format the data
def transform(df):
    df.dropna(inplace=True)  # Remove missing values
    df['price'] = df['price'].astype(float)  # Convert price to float
    df['date'] = pd.to_datetime(df['date'])  # Convert date column
    return df

# Step 3: Load - Load data into PostgreSQL
def load(df, table_name, db_url):
    engine = create_engine(db_url)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Data loaded successfully!")

if __name__ == "__main__":
    csv_file = "data.csv"  # Replace with actual CSV file path
    db_url = "postgresql://user:password@localhost:5432/mydatabase"  # Replace with actual DB credentials
    table_name = "car_prices"
    
    # Run ETL pipeline
    data = extract(csv_file)
    transformed_data = transform(data)
    load(transformed_data, table_name, db_url)
