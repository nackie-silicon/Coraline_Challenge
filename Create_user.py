import psycopg2

import configparser

config = configparser.ConfigParser()
config.read('config.ini')


try:
    connection = psycopg2.connect(
                user = "postgres",
                password = "password",
                host = "localhost",
                port = "5432",
                database = "postgres")

    connection.autocommit = True
    cursor = connection.cursor()

    user = config.get('Database', 'username')
    new_pwd = config.get('Database', 'password')

    create_user_sql = f'''CREATE USER {user} WITH LOGIN SUPERUSER PASSWORD '{new_pwd}'  '''
    cursor.execute(create_user_sql)

    connection.close()
    print('Create user successfully completed!')


except Exception as error:
  print("An error occurred:", type(error).__name__) # An error occurred: NameError