import pandas
import time
import config

def pandas_test():

    results = [0, 0, 0, 0]

    df = pandas.read_csv(config.path_to_file_csv)

    for i in range(config.number_of_tests):
        seconds_start = time.time()

        selected_df = df[['VendorID']]
        grouped_df = selected_df.groupby('VendorID')
        final_df = grouped_df.size().reset_index(name='counts')

        seconds_finish = time.time()
        results[0] += (seconds_finish - seconds_start)
    results[0] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()

        selected_df = df[['passenger_count', 'total_amount']]
        grouped_df = selected_df.groupby('passenger_count')
        final_df = grouped_df.mean().reset_index()

        seconds_finish = time.time()
        results[1] += (seconds_finish - seconds_start)
    results[1] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()

        #df = pandas.read_csv("nyc_yellow_tiny.csv")
        selected_df = df.loc[:, ['passenger_count', 'tpep_pickup_datetime']]    #.loc
        selected_df['year'] = pandas.to_datetime(
            selected_df.pop('tpep_pickup_datetime'),
            format='%Y-%m-%d %H:%M:%S').dt.year
        grouped_df = selected_df.groupby(['passenger_count', 'year'])
        final_df = grouped_df.size().reset_index(name='counts')

        seconds_finish = time.time()
        results[2] += (seconds_finish - seconds_start)
    results[2] /= config.number_of_tests

    for i in range(config.number_of_tests):
        seconds_start = time.time()

        selected_df = df.loc[:, [
            'passenger_count',
            'tpep_pickup_datetime',
            'trip_distance']]
        selected_df['trip_distance'] = selected_df['trip_distance'].round().astype(int)
        selected_df['year'] = pandas.to_datetime(
            selected_df.pop('tpep_pickup_datetime'),
            format='%Y-%m-%d %H:%M:%S').dt.year
        grouped_df = selected_df.groupby([
            'passenger_count',
            'year',
            'trip_distance'])
        final_df = grouped_df.size().reset_index(name='counts')
        final_df = final_df.sort_values(
            ['year', 'counts'],
            ascending=[True, False])

        seconds_finish = time.time()
        results[3] += (seconds_finish - seconds_start)
    results[3] /= config.number_of_tests

    print("Pandas test:")
    for i in range(4):
        print(f"Query {i}: {results[i]}")
    print()