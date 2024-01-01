# Import libraries
import psycopg2
import openpyxl

import pandas as pd
import numpy as np

import configparser

config = configparser.ConfigParser()
config.read('config.ini')


try:
    # Create a connection to existing DB
    connection = psycopg2.connect(
    database = config.get('Database', 'database'),
    user = config.get('Database', 'username'),
    password = config.get('Database', 'password'),
    host = config.get('Database', 'host'),
    port = config.get('Database', 'port')
    )

    # Open a cursor object to perform database operations
    cursor = connection.cursor()

    # Save Excel file location into a variable
    excel_file = 'de_challenge_data.xlsx'

    # Open the Excel workbook and load the active sheet into a variable
    workbook = openpyxl.load_workbook(excel_file, data_only=True)
    sheet = workbook.active

    df = pd.DataFrame(sheet.values)

    # Split row by empty column
    nan_indices = df.index[df.isna().all(axis=1)]
    df_list = [df.dropna() for df in np.split(df, nan_indices)]
    a=df_list[0]
    b=df_list[1]

    # Remove duplicate heading column
    b1 = b.iloc[1:]

    # Concatenate row
    result = pd.concat([a, b1], axis=0, ignore_index=True)

    # Assign heading column
    result.columns = result.iloc[0]
    result = result[1:]

    # Create a list with the column names in the first row
    column_names = list(result.columns.values)

    # Create an empty list 
    data = []

    # Create a loop to iterate over all rows in the excel sheet (except the titles) and append the data to the list
    for row in result.values:  # iter_rows is an openpyxl function
        data.append(row)

    # Set a name for the postgreSQL schema and table where we will put the data
    schema_name = 'FoodSales'
    table_name = 'food_sales'
    table_name_target = 'cat_reg'


    # Write a query to create a schema using schema_name. Save it into a variable.
    schema_creation_query = f'CREATE SCHEMA IF NOT EXISTS {schema_name}'

    # Write a query to create a table (table_name) in the schema (schema_name). It must contain all columns in column_names. Save it into a variable.
    table_creation_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
    {", ".join([f'"{name}" TEXT' for name in column_names])}
    )
    """

    # Write a query to create a target table
    table_creation_query_2 =f""" 
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name_target}( 
                    Category VARCHAR, 
                    East INTEGER, 
                    West INTEGER,
                    TotalPrice INTEGER
            ) 
            """

    # Use the cursor to execute both queries
    cursor.execute(schema_creation_query)
    cursor.execute(table_creation_query)
    cursor.execute(table_creation_query_2)

    # Create a parameterized SQL query to insert the data into the table
    insert_data_query = f"""
        INSERT INTO {schema_name}.{table_name} ({", ".join([f'"{name}"' for name in column_names])})
        VALUES ({", ".join(['%s' for _ in column_names])})
    """

    # Execute the query using the data list as parameter
    cursor.executemany(insert_data_query, data)

    # Select data for target table
    sql_query = f""" select TEMP."Category", ROUND(TEMP."East",0) AS "East" , ROUND(TEMP."West",0) AS "West" , ROUND((TEMP."East" + TEMP."West"),0) AS "TotalPrice"  from 
    (
    select  "Category", 
    SUM(CASE WHEN"Region" = 'East' THEN ("Qty"::INTEGER)*("UnitPrice"::NUMERIC) END) AS "East",
    SUM(CASE WHEN"Region" = 'West' THEN ("Qty"::INTEGER)*("UnitPrice"::NUMERIC) END) AS "West"
    from {schema_name}.{table_name}   
    group by "Category" ) AS TEMP order by TEMP."Category" ; """

    cursor.execute(sql_query)

    target_data = []

    result = cursor.fetchall()
    for row in result:
        target_data.append(row)


    insert_target_query = f"""
        INSERT into {schema_name}.{table_name_target} ( Category, East, West, TotalPrice) VALUES (%s, %s, %s, %s)
    """

    cursor.executemany(insert_target_query, target_data)


    # Make the changes to the database persistent
    connection.commit()


    # Close communication with the database
    cursor.close()
    connection.close()
    # Print a message
    print('Import successfully completed!')

except Exception as error:
  print("An error occurred:", type(error).__name__) # An error occurred: NameError








