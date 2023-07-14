if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import psycopg2


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
def transform_custom(data, dim_source):

    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")
    cur = conn.cursor()
    for index, row in dim_source.iterrows():
        try:
            conn.autocommit = False
            
            command = f"""
                        INSERT INTO dimsource
                        VALUES ({row["dim_source_id"]}, '{row["Source"]}')
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
