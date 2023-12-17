import psycopg2
import pandas
from sqlalchemy import create_engine
import config
import time
import os

def postgres_test():

    if config.create_db_postgres and os.path.exists(config.path_to_file_csv):
        connection = psycopg2.connect(
            dbname=config.db_name,
            host=config.host,
            user=config.user,
            password=config.password,
            port=config.port
        )
        engine = create_engine(f'postgresql+psycopg2://{config.user}:{config.password}@localhost:{config.port}/taxi')
        db = engine.connect()
        df = pandas.read_csv(config.path_to_file_csv)
        df.to_sql('taxi', con=db, if_exists='replace', index=False, chunksize=1000, method='multi')
        db.close()

    results = [0, 0, 0, 0]

    connection = psycopg2.connect(
        dbname=config.db_name,
        host=config.host,
        user=config.user,
        password=config.password,
        port=config.port
    )
    cursor = connection.cursor()

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        cursor.execute(f'''SELECT \"VendorID\", count(*)
                          FROM {config.db_name}
                          GROUP BY 1;''')
        seconds_finish = time.time()

        results[0] += (seconds_finish - seconds_start)
    results[0] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        cursor.execute(f'''SELECT passenger_count, avg(total_amount) 
                          FROM {config.db_name}
                          GROUP BY 1;''')
        seconds_finish = time.time()

        results[1] += (seconds_finish - seconds_start)
    results[1] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        cursor.execute(f'''SELECT passenger_count, extract(year from TO_TIMESTAMP(tpep_pickup_datetime,'YYYY/MM/DD/HH24:MI:ss')), count(*)
                          FROM {config.db_name}
                          GROUP BY 1, 2;''')
        seconds_finish = time.time()

        results[2] += (seconds_finish - seconds_start)
    results[2] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        cursor.execute(f'''SELECT passenger_count, extract(year from TO_TIMESTAMP(tpep_pickup_datetime,'YYYY/MM/DD/HH24:MI:ss')), round(trip_distance), count(*)
                          FROM {config.db_name}
                          GROUP BY 1, 2, 3
                          ORDER BY 2, 4 desc;''')
        seconds_finish = time.time()

        results[3] += (seconds_finish - seconds_start)
    results[3] /= config.number_of_tests

    cursor.close()
    connection.close()

    print("Postgres test:")
    for i in range(4):
        print(f"Query {i}: {results[i]}")
    print()