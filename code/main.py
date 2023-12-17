import postgres_
import sqlalchemy_
import sqlite_
import duckdb_
import pandas_
import config

if config.start_postgres_test:
    postgres_.postgres_test()
if config.start_sqlite_test:
    sqlite_.sqlite_test()
if config.start_duckdb_test:
    duckdb_.duckdb_test()
if config.start_pandas_test:
    pandas_.pandas_test()
if config.start_sqlalchemy_test:
    sqlalchemy_.sqlalchemy_test()