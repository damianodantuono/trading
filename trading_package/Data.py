import sqlite3
import pandas as pd
import pandas_datareader as pdr


class Data():

    _existQuery = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
    _createTableQuery = """CREATE TABLE IF NOT EXISTS {tableName} (
	ID identity(1,1),
   	TIMESTAMP DATETIME NOT NULL,
	OPEN float,
   	HIGH float,
	LOW float,
	CLOSE float,
	ADJ_CLOSE float,
	VOLUME integer
);
"""
    _readDataQuery = """SELECT DATE, "OPEN", HIGH, LOW, "CLOSE", ADJ_CLOSE, VOLUME
FROM {tableName}
"""
    _emptyQuery = "SELECT COUNT(*) FROM {tableName}"

    def _connect(database):
        return sqlite3.connect(database)

    def _disconnect(connection):
        connection.close()

    def exists(database, ticker):
        conn = Data._connect(database)
        try:
            cursor = conn.cursor()
            cursor.execute(Data._existQuery, (ticker, ))
            fetched = cursor.fetchall()
        finally:
            Data._disconnect(conn)
        return bool(fetched)

    def empty(database, ticker):
        conn = Data._connect(database)
        try:
            cursor = conn.cursor()
            cursor.execute(Data._emptyQuery.format(tableName=ticker))
            fetched = cursor.fetchall()
        finally:
            Data._disconnect(conn)
        return not bool(fetched[0][0])

    def createTable(database, ticker):
        conn = Data._connect(database)
        try:
            cursor = conn.cursor()
            cursor.execute(Data._createTableQuery.format(tableName=ticker))
            print(cursor.fetchall())
        finally:
            Data._disconnect(conn)

    def insertData(database, ticker, data):
        conn = Data._connect(database)
        try:
            data.to_sql(ticker, con=conn, if_exists='replace', index=False)
        finally:
            Data._disconnect(conn)

    def loadDataframe(database, ticker):
        conn = Data._connect(database)
        try:
            df = pd.read_sql_query(Data._readDataQuery.format(tableName=ticker),
                                   con=conn, index_col="DATE")
        finally:
            Data._disconnect(conn)
        return df
