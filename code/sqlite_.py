import sqlite3
import pandas
from sqlalchemy import create_engine
import config
import time
import os

def sqlite_test():

    if config.create_db_sqlite and os.path.exists(config.path_to_file_csv) and not os.path.exists(config.path_to_file_sqlite):
        connection = sqlite3.connect(config.path_to_file_sqlite)
        engine = create_engine(f"sqlite:///{config.path_to_file_sqlite}")
        db = engine.connect()
        df = pandas.read_csv(config.path_to_file_csv)
        df.to_sql('taxi', con=db, if_exists='replace', index=False, chunksize=1000, method='multi')
        db.close()

    results = [0, 0, 0, 0]

    connection = sqlite3.connect(config.path_to_file_sqlite)
    cursor = connection.cursor()

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        cursor.execute(f'''SELECT \"VendorID\", count(*)
                          FROM taxi
                          GROUP BY 1;''')
        seconds_finish = time.time()

        results[0] += (seconds_finish - seconds_start)
    results[0] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        cursor.execute(f'''SELECT passenger_count, avg(total_amount) 
                          FROM taxi
                          GROUP BY 1;''')
        seconds_finish = time.time()

        results[1] += (seconds_finish - seconds_start)
    results[1] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        cursor.execute(f'''SELECT passenger_count, strftime("%Y", tpep_pickup_datetime), count(*)
                          FROM taxi
                          GROUP BY 1, 2;''')
        seconds_finish = time.time()

        results[2] += (seconds_finish - seconds_start)
    results[2] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        cursor.execute(f'''SELECT passenger_count, strftime("%Y", tpep_pickup_datetime), round(trip_distance), count(*)
                          FROM taxi
                          GROUP BY 1, 2, 3
                          ORDER BY 2, 4 desc;''')
        seconds_finish = time.time()

        results[3] += (seconds_finish - seconds_start)
    results[3] /= config.number_of_tests

    cursor.close()
    connection.close()

    print("SQLite test:")
    for i in range(4):
        print(f"Query {i}: {results[i]}")
    print()