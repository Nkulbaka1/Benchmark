import duckdb
import config
import time

def duckdb_test():

    results = [0, 0, 0, 0]

    duckdb.read_csv(config.path_to_file_csv)

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        duckdb.execute(f'''SELECT \"VendorID\", count(*)
                          FROM '{config.path_to_file_csv}'
                          GROUP BY 1;''')
        seconds_finish = time.time()

        results[0] += (seconds_finish - seconds_start)
    results[0] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        duckdb.execute(f'''SELECT passenger_count, avg(total_amount) 
                          FROM '{config.path_to_file_csv}'
                          GROUP BY 1;''')
        seconds_finish = time.time()

        results[1] += (seconds_finish - seconds_start)
    results[1] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        duckdb.execute(f'''SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*)
                          FROM '{config.path_to_file_csv}'
                          GROUP BY 1, 2;''')
        seconds_finish = time.time()

        results[2] += (seconds_finish - seconds_start)
    results[2] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()
        duckdb.execute(f'''SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*)
                          FROM '{config.path_to_file_csv}'
                          GROUP BY 1, 2, 3
                          ORDER BY 2, 4 desc;''')
        seconds_finish = time.time()

        results[3] += (seconds_finish - seconds_start)
    results[3] /= config.number_of_tests

    print("DuckDB test:")
    for i in range(4):
        print(f"Query {i}: {results[i]}")
    print()