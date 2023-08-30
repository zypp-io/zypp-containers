import pandas as pd
from database import execute_query

bool_mapping = {
    "Ja": True,
    "Nee": False,
    "Aan": True,
    "Uit": False,
    "True": True,
    "False": False,
}

type_mapping = {
    "int": int,
    "bool": bool,
    "str": str,
    "float": float,
    "list": list
}


def parse_param(row):
        type_name = row['type']
        row_value = row['value']

        try:
            type_constructor = type_mapping[type_name]
        except KeyError:
            raise NameError(f"Type {type_name} not found")
        
        if type_name == 'bool':
            try:
                param_value = bool_mapping[row_value]
            except KeyError:
                raise ValueError(f"Value {row_value} not recognized for bool type")
        else:
            param_value = type_constructor(row_value)
        return param_value

def get_container_params(container_name, schema="dataportaal") -> dict:
    result = execute_query(f"""
                        SELECT * from {schema}.container
                        left join {schema}.container_params on container.container_id = container_params.container_id
                        left join {schema}.container_param_options on container_param_options.param_id = container_params.param_id
                        WHERE container_name = '{container_name}' and chosen_option_id = option_id
                        """)

    df = pd.DataFrame(result)
    df['parsed_param_value'] = df.apply(parse_param, axis=1)
    params = {row['name']: row['parsed_param_value'] for _, row in df.iterrows()} 

    return params