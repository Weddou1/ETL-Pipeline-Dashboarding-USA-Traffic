if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test



import psycopg2
import math


def convert_nan_to_null(value):
    return 'NULL' if math.isnan(value) else value

@custom
def transform_custom(data, dim_airport_code):
    """
    Index(['dim_airport_code_id', 'Airport_Code'], dtype='object')


    """
    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")
    cur = conn.cursor()
    for index, row in dim_airport_code.iterrows():
        try:
            conn.autocommit = False

            #row["Airport_Code"] = convert_nan_to_null(row["Airport_Code"]) 
            
            
            command = f"""
                        INSERT INTO Dimairportcode
                        VALUES ({row["dim_airport_code_id"]}, '{row["Airport_Code"]}')
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
