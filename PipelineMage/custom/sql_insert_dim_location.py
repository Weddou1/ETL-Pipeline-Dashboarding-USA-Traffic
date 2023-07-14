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
def transform_custom(data, dim_location):
    """
    Index(['dim_location_id', 'Street', 'City', 'Zipcode', 'State', 'County',
       'Country', 'Timezone'],
      dtype='object')


    """
    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")
    cur = conn.cursor()
    for index, row in dim_location.iterrows():
        try:
            conn.autocommit = False

            row["Street"] = escape_single_quotes(row["Street"]) 
            row["City"] = escape_single_quotes(row["City"]) 
            row["Zipcode"] = escape_single_quotes(row["Zipcode"]) 
            row["State"] = escape_single_quotes(row["State"]) 
            row["County"] = escape_single_quotes(row["County"]) 
            row["Country"] = escape_single_quotes(row["Country"]) 
            row["Timezone"] = escape_single_quotes(row["Timezone"]) 
            
            
            command = f"""
                        INSERT INTO Dimlocation
                        VALUES ({row["dim_location_id"]}, '{row["Street"]}','{row["City"]}','{row["Zipcode"]}',
                        '{row["State"]}','{row["County"]}','{row["Country"]}','{row["Timezone"]}')
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
