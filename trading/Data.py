import sqlite3
import pandas as pd
import pandas_datareader as pdr


class Data:
    _existQuery = "SELECT name FROM sqlite_master WHERE type='table' AND name='{tableName}';"
    _createTableQuery = """CREATE TABLE IF NOT EXISTS "{tableName}" (
   	DATE DATETIME NOT NULL,
	OPEN float,
   	HIGH float,
	LOW float,
	CLOSE float,
	ADJ_CLOSE float,
	VOLUME integer,
    UP_DONCHIAN float,
    DOWN_DONCHIAN float
);
"""
    _readDataQuery = """SELECT DATE, "OPEN", HIGH, LOW, "CLOSE", ADJ_CLOSE, VOLUME
FROM "{tableName}"
"""
    _emptyQuery = "SELECT COUNT(*) FROM \"{tableName}\""
    _dropQuery = "DROP TABLE IF EXISTS \"{tableName}\""

    def __init__(self, database, ticker):
        self.database = database
        self.ticker = ticker

    def _connect(self):
        return sqlite3.connect(self.database)

    def _disconnect(self, connection):
        connection.close()

    def exists(self):
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._existQuery.format(tableName=self.ticker))
            fetched = cursor.fetchall()
        finally:
            self._disconnect(conn)
        return bool(fetched)

    def empty(self):
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._emptyQuery.format(tableName=self.ticker))
            fetched = cursor.fetchall()
        finally:
            self._disconnect(conn)
        return not bool(fetched[0][0])

    def createTable(self):
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._createTableQuery.format(
                tableName=self.ticker))
        finally:
            self._disconnect(conn)

    def insertData(self):
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {self.ticker};")
            self.fetchData().to_sql(self.ticker, con=conn,
                                    if_exists='append', index=False)
        finally:
            self._disconnect(conn)

    def loadDataframe(self):
        conn = self._connect()
        try:
            df = pd.read_sql_query(self._readDataQuery.format(tableName=self.ticker),
                                   con=conn, index_col="DATE")
        finally:
            self._disconnect(conn)
        return df

    def fetchData(self):
        return pdr.get_data_yahoo(self.ticker).reset_index(level=0).rename(
            columns={"Date": "DATE", "High": "HIGH", "Low": "LOW", "Open": "OPEN", "Close": "CLOSE", "Volume": "VOLUME",
                     "Adj Close": "ADJ_CLOSE"})[["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

    def dropTable(self):
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._dropQuery.format(
                tableName=self.ticker))
        finally:
            self._disconnect(conn)
