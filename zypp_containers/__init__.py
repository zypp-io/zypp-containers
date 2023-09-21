import os
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
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


def make_engine():
    connection_string = create_connection_string()
    engine = create_engine(connection_string, echo=False, future=True)

    return engine


def execute_query(query):
    """Executes a query on the database and fetches and returns the result"""
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchall()
        return result


engine = make_engine()
db_session = sessionmaker(bind=engine)()


bool_mapping = {
    "Ja": True,
    "Nee": False,
    "Aan": True,
    "Uit": False,
    "True": True,
    "False": False,
}

type_mapping = {"int": int, "bool": bool, "str": str, "float": float, "list": list}


def parse_param(row):
    """
    Accepts a database row from the container_params table
    and parses the row to a correct parameter using the type column
    """
    type_name = row["type"]
    row_value = row["value"]

    try:
        type_constructor = type_mapping[type_name]
    except KeyError:
        raise NameError(f"Type {type_name} not found")

    if type_name == "bool":
        try:
            param_value = bool_mapping[row_value]
        except KeyError:
            raise ValueError(f"Value {row_value} not recognized for bool type")
    else:
        param_value = type_constructor(row_value)
    return param_value


def get_container_params(container_name, schema="dataportaal") -> dict:
    """
    Get the parameters for a container from the database by the container name
    """
    result = execute_query(
        f"""
                        SELECT * from {schema}.container
                        left join {schema}.container_params on container.container_id = container_params.container_id
                        left join {schema}.container_param_options on
                        container_param_options.param_id = container_params.param_id
                        WHERE container_name = '{container_name}' and chosen_option_id = option_id
                        """
    )

    df = pd.DataFrame(result)
    df["parsed_param_value"] = df.apply(parse_param, axis=1)
    params = {row["name"]: row["parsed_param_value"] for _, row in df.iterrows()}

    return params
