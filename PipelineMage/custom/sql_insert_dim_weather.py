if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import psycopg2
import math



def convert_nan_to_null(value):
    if isinstance(value, (int, float)):
        if math.isnan(value):
            return 'null'
        else:
            return value
    elif isinstance(value, str):
        if value.lower() == 'nan':
            return 'null'
        else:
            return value
    else:
        return value

def convert_date_nan_to_null(value):
    if isinstance(value, (int, float)):
        if math.isnan(value):
            return '1700-01-01'
        else:
            return value
    elif isinstance(value, str):
        if value.lower() == 'nan' or value.lower() == 'none':
            return '1700-01-01'
        else:
            return value
    elif value is None:
        return '1700-01-01'
    else:
        return value



def escape_single_quotes(value):
    if isinstance(value, str):
        return value.replace("'", "''")
    return value


@custom
def transform_custom(data, dim_weather):
    """
    Index(['Weather_Timestamp', 'dim_weather_cond_id'], dtype='object')




    """
    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")
    cur = conn.cursor()
    for index, row in dim_weather.iterrows():
        try:
            conn.autocommit = False
            
            row["Weather_Timestamp"]=convert_date_nan_to_null(row["Weather_Timestamp"])
            

            command = f"""
                        INSERT INTO Dimweather
                        VALUES ('{row["Weather_Timestamp"]}', 
                                {row["dim_weather_cond_id"]})
                        ON CONFLICT DO NOTHING;
                    """

            cur.execute(command)

            conn.commit()
        except psycopg2.Error as e:
            print(row["Weather_Timestamp"])
            print(command)
            conn.rollback()
            print("Error: ", e)
        finally:
            conn.autocommit = True

    cur.close()
    conn.close()




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
