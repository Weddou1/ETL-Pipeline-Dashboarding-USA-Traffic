if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import psycopg2

@custom
def transform_custom(data, dim_cond_wind):

    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")
    cur = conn.cursor()
    for index, row in dim_cond_wind.iterrows():
        try:
            conn.autocommit = False
            
            command = f"""
                        INSERT INTO Dimcondwind
                        VALUES ({row["dim_cond_wind_id"]}, '{row["Wind_Direction"]}')
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
