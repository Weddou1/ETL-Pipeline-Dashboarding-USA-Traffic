if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import psycopg2
import math


def convert_nan_to_null(value):
    return '' if math.isnan(value) else value

def convert_date_nan_to_null(value):
    return '1700-01-01' if math.isnan(value) else value


def escape_single_quotes(value):
    if isinstance(value, str):
        return value.replace("'", "''")
    return value

@custom
def transform_custom(data, fact_accidents):
    """
    Index(['ID', 'Severity', 'Start_Time', 'End_Time', 'End_Lat', 'End_Lng',
       'Distance(mi)', 'Description', 'Temperature(F)', 'Humidity(%)',
       'Pressure(in)', 'Visibility(mi)', 'binary_day', 'binary_road_attribute',
       'start_lat', 'start_lng', 'Weather_Timestamp', 'dim_source_id',
       'dim_wind_id'],
      dtype='object')


    """




    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")
    cur = conn.cursor()
    for index, row in fact_accidents.iterrows():
        try:
            conn.autocommit = False
            
            row['Severity']=convert_nan_to_null(row['Severity'])
            row['Start_Time']=convert_nan_to_null(row['Start_Time'])
            row['End_Time']=convert_nan_to_null(row['End_Time'])
            row['End_Lat']=convert_nan_to_null(row['End_Lat'])
            row['End_Lng']=convert_nan_to_null(row['End_Lng'])
            row['Distance(mi)']=convert_nan_to_null(row['Distance(mi)'])
            row['Description']=convert_nan_to_null(row['Description'])
            row['Temperature(F)']=convert_nan_to_null(row['Temperature(F)'])
            row['Humidity(%)']=convert_nan_to_null(row['Humidity(%)'])
            row['Pressure(in)']=convert_nan_to_null(row['Pressure(in)'])
            row['Visibility(mi)']=convert_nan_to_null(row['Visibility(mi)'])
            row['binary_day']=convert_nan_to_null(row['binary_day'])
            row['binary_road_attribute']=convert_nan_to_null(row['binary_road_attribute'])
            row['Weather_Timestamp']=convert_date_nan_to_null(row['Weather_Timestamp'])

            
            command = f"""
                        INSERT INTO factaccident
                        VALUES ('{row["ID"]}', 
                                '{row["Severity"]}', 
                                '{row["Start_Time"]}', 
                                '{row["End_Time"]}', 
                                {row["start_lat"]}, 
                                {row["start_lng"]},
                                {row["end_lat"]}, 
                                {row["end_lng"]},
                                {row['Distance(mi)']},
                                '{row["End_Time"]}',
                                '{row['Description']})',
                                {row['Temperature(F)']},
                                {row['Humidity(%)']},
                                {row['Pressure(in)']},
                                {row['Visibility(mi)']},
                                '{row['binary_day']})',
                                '{row['binary_road_attribute']})',
                                {row['Pressure(in)']},
                                {row['dim_wind_id']},
                                {row['dim_source_id']},
                                {row['Weather_Timestamp']}
                                
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
