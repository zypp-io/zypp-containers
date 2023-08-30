import os
from urllib.parse import quote_plus

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import sessionmaker


def create_connection_string():
    connection_string = "mssql+pyodbc://{}:{}@{}:1433/{}?driver={}".format(
        os.environ["SQL_USER"],
        quote_plus(os.environ["SQL_PW"]),
        os.environ["SQL_SERVER"],
        os.environ["SQL_DB"],
        "ODBC Driver 17 for SQL Server",
    )
    return connection_string


def make_engine() -> Engine:
    connection_string = create_connection_string()
    engine = create_engine(connection_string, echo=False, future=True)

    return engine


def execute_query(query):
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchall()
        return result


engine = make_engine()
db_session = sessionmaker(bind=engine)()
