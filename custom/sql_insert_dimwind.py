if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import psycopg2
import math


def convert_nan_to_null(value):
    return 'NULL' if math.isnan(value) else value

@custom
def transform_custom(data, dim_wind):
    """
    Index(['dim_wind_id', 'dim_cond_wind_id', 'Wind_Chill(F)', 'Wind_Speed(mph)',
       'Precipitation(in)'],

    """
    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")
    cur = conn.cursor()
    for index, row in dim_wind.iterrows():
        try:
            conn.autocommit = False

            row["Wind_Chill(F)"] = convert_nan_to_null(row["Wind_Chill(F)"])
            row["Wind_Speed(mph)"] = convert_nan_to_null(row["Wind_Speed(mph)"]) 
            row["Precipitation(in)"] = convert_nan_to_null(row["Precipitation(in)"]) 
            
            
            command = f"""
                        INSERT INTO Dimwind
                        VALUES ({row["dim_wind_id"]}, {row["Wind_Chill(F)"]},{row["Wind_Speed(mph)"]}, {row["Precipitation(in)"]},{row["dim_cond_wind_id"]})
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
