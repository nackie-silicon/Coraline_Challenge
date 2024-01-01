import psycopg2

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

try:
    #Creating a postgresql database
    connection = psycopg2.connect(
            user = config.get('Database', 'username'),
            password = config.get('Database', 'password'),
            host = "localhost",
            port = "5432",
            database = "postgres")

    connection.autocommit = True
    cursor = connection.cursor()

    
    #SQL Code Block
    sql = '''CREATE DATABASE challenge'''

    cursor.execute(sql)
    connection.close()

    print('Create database successfully completed!')

except Exception as error:
  print("An error occurred:", type(error).__name__) # An error occurred: NameError