if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import psycopg2
import math


def convert_nan_to_null(value):
    return '' if math.isnan(value) else value

def escape_single_quotes(value):
    if isinstance(value, str):
        return value.replace("'", "''")
    return value

@custom
def transform_custom(data, dim_weather_cond):
    """
    Index(['dim_weather_cond_id', 'Weather_Condition'], dtype='object')
')



    """
    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")
    cur = conn.cursor()
    for index, row in dim_weather_cond.iterrows():
        try:
            conn.autocommit = False
            
            
            command = f"""
                        INSERT INTO Dimweathercond
                        VALUES ({row["dim_weather_cond_id"]}, 
                                '{row["Weather_Condition"]}')
                        ON CONFLICT DO NOTHING;
                    """
            cur.execute(command)

            conn.commit()
        except psycopg2.Error as e:
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
