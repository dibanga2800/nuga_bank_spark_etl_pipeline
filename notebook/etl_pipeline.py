#importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
import os
import psycopg2

#set Java home
os.environ['JAVA_HOME'] = r'c:\Program Files\Java\jdk-17'

#Initialize Spark session
spark = SparkSession.builder \
    .appName("Nuga Bank ETL Pipeline") \
        .config("spark.jars", r"c:\Users\David Ibanga\Data Engineering practicals\nuga_bank_etl_case_study\postgresql-42.7.7.jar") \
        .getOrCreate()


#Extract this historical data into a Spark DataFrame
df = spark.read.csv(r'..\dataset\rawdata\nuga_bank_transactions.csv', header=True, inferSchema=True)

#fill up the missing values
df_clean = df.fillna({
    'Customer_Name': 'Unknown',
    'Customer_Address': 'Unknown',
    'Customer_City': 'Unknown',
    'Customer_State': 'Unknown',
    'Customer_Country': 'Unknown',
    'Company': 'Unknown',
    'Job_Title': 'Unknown',
    'Email': 'Unknown',
    'Phone_Number': 'Unknown',
    'Credit_Card_Number': 0,
    'IBAN': 'Unknown',
    'Currency_Code':'Unknown',
    'Random_Number': '0.0',
    'Category': 'Unknown',
    'Group': 'Unknown',
    'Is_Active': 'Unknown',
    'Last_Updated': 'Unknown',
    'Description': 'Unknown',
    'Gender': 'Unknown',
    'Marital_Status': 'Unknown'
})



#Drop missing values in the last_updated column
df_clean=df_clean.dropna(subset=['Last_Updated'])



#Data Transformation to 2NF
#Transaction Table
transaction = df_clean.select(
    'Transaction_Date','Amount','Transaction_Type') \
    .withColumn('transaction_id', monotonically_increasing_id()) \
    .select('transaction_id', 'Transaction_Date', 'Amount', 'Transaction_Type')


#customer Table
customer = df_clean.select('Customer_Name', 'Customer_Address','Customer_City', 'Customer_State', 'Customer_Country') \
    .withColumn('customer_id', monotonically_increasing_id()) \
    .select('customer_id', 'Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country')

#Emploee Table
employee = df_clean.select('Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status') \
    .withColumn('employee_id', monotonically_increasing_id()) \
    .select('employee_id', 'Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status')

#Nuga_bank_fact_table
fact_table = df_clean.join(transaction, [ 'Transaction_Date', 'Amount', 'Transaction_Type'], 'inner') \
    .join(customer, ['Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country'], 'inner') \
    .join(employee, ['Company', 'Job_Title', 'Email', 'Phone_Number', 'Gender', 'Marital_Status'], 'inner') \
    .select('transaction_id', 'customer_id', 'employee_id','Credit_Card_Number','IBAN', 'Currency_Code', 'Random_Number','Category','Group','Is_Active', 'Last_Updated','Description')
   


#Data Loading
# Save to PostgreSQL
import psycopg2
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()

# PostgreSQL credentials
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USERNAME', 'postgres'),  # Changed from DB_USER to match JDBC properties
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
    

  
    
# Connect to sql database
conn = get_db_connection()

#Create a function to create tables
def create_table():
    conn = get_db_connection()
    if conn is None:
        print("Failed to connect to database. Cannot create tables.")
        return False
        
    cursor = conn.cursor()
    create_table_query = '''
                    
                      DROP TABLE IF EXISTS fact_table;
                      DROP TABLE IF EXISTS customer;
                      DROP TABLE IF EXISTS transaction;
                      DROP TABLE IF EXISTS employee;

                   

                    CREATE TABLE customer (
                        customer_id BIGINT PRIMARY KEY,
                        customer_name VARCHAR(10000),
                        customer_address VARCHAR(10000),
                        customer_city VARCHAR(10000),
                        customer_state VARCHAR(10000),
                        customer_country VARCHAR(10000)
                    );

                    CREATE TABLE transaction (
                         transaction_id BIGINT PRIMARY KEY,
                         transaction_date DATE,
                         amount FLOAT,
                         transaction_type VARCHAR(10000)
                        );

                    CREATE TABLE employee (
                        employee_id BIGINT PRIMARY KEY,
                        company VARCHAR(10000),
                        job_title VARCHAR(10000),
                        email VARCHAR(10000),
                        phone_number VARCHAR(10000),
                        gender VARCHAR(10000),
                        marital_status VARCHAR(10000)
                    );
                    CREATE TABLE fact_table (
                        transaction_id BIGINT REFERENCES transaction(transaction_id),
                        customer_id BIGINT REFERENCES customer(customer_id),
                        employee_id BIGINT REFERENCES employee(employee_id),
                        credit_card_number VARCHAR(10000),
                        iban VARCHAR(10000),
                        currency_code VARCHAR(10000),
                        random_number FLOAT,
                        category VARCHAR(10000),
                        "group" VARCHAR(10000),
                        is_active VARCHAR(10000),
                        last_updated TIMESTAMP,
                        description VARCHAR(10000)
                    );
                    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully in PostgreSQL database.")
    return True


# Create tables in the database
if not create_table():
    print("Stopping execution due to table creation failure.")
    exit(1)

from dotenv import load_dotenv
import os

load_dotenv()

url = f"jdbc:postgresql://localhost:5432/{os.getenv('DB_NAME')}"
properties = {
    "user": os.getenv('DB_USERNAME'),
    "password": os.getenv('DB_PASSWORD'),
    "driver": "org.postgresql.Driver"
}

# Write data to PostgreSQL with error handling
try:
    print("Writing customer data...")
    customer.write.jdbc(
        url=url,
        table="customer",
        mode="append",
        properties=properties
    )
    print("Customer data written successfully.")

    print("Writing transaction data...")
    transaction.write.jdbc(
        url=url,
        table='"transaction"',  # quoted because it's a reserved keyword
        mode="append",
        properties=properties
    )
    print("Transaction data written successfully.")

    print("Writing employee data...")
    employee.write.jdbc(
        url=url,
        table="employee",
        mode="append",
        properties=properties
    )
    print("Employee data written successfully.")

    print("Writing fact table data...")
    fact_table.write.jdbc(
        url=url,
        table="fact_table",
        mode="append",
        properties=properties
    )
    print("Fact table data written successfully.")

except Exception as e:
    print(f"Error writing data to PostgreSQL: {e}")
    raise

print("Database tables and data loaded successfully into PostgreSQL tables.")