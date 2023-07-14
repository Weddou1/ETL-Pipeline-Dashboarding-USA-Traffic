import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def get_unique_values(df,columns):
    return df[columns].drop_duplicates().reset_index(drop=True)

def get_id_column(df,column_name):
    return df.insert(0, column_name, range(1, len(df) + 1))

@transformer
def transform(df, dim_volatile_day_period, dim_volatile_road_attribute,dim_source,  dim_cond_wind, dim_wind):

    fact_table=['ID', 'Severity', 'Start_Time', 'End_Time',  'End_Lat', 'End_Lng', 'Distance(mi)', 'Description',
       'Temperature(F)',
       'Humidity(%)', 'Pressure(in)', 'Visibility(mi)']
    fact_table_day=['Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight',
        'Astronomical_Twilight']
    fact_table_road=['Amenity',
        'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
        'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal',
        'Turning_Loop']
        
    fact_accidents=get_unique_values(df,fact_table + fact_table_day + fact_table_road)
    print("test 2")

    fact_accidents=fact_accidents.merge(dim_volatile_road_attribute,on=fact_table_road,how="left")
    
    fact_accidents=fact_accidents.merge(dim_volatile_day_period,on=fact_table_day,how="left")
    
    selected_columns = list(range(0, 12)) + [-1, -2]
    fact_accidents = fact_accidents.iloc[:, selected_columns]


    fact_accidents["start_lat"]=df["Start_Lat"]
    fact_accidents["start_lng"]=df["Start_Lng"]
    fact_accidents["Weather_Timestamp"]=df["Weather_Timestamp"]
    fact_accidents["Source"]=df["Source"]
    fact_accidents["Wind_Direction"]=df["Wind_Direction"]
    fact_accidents["Wind_Chill(F)"]=df["Wind_Chill(F)"]
    fact_accidents["Wind_Speed(mph)"]=df["Wind_Speed(mph)"]
    fact_accidents["Precipitation(in)"]=df["Precipitation(in)"]


    fact_accidents=fact_accidents.merge(dim_source,on="Source",how="left").merge(dim_cond_wind,on="Wind_Direction",how="left").merge(dim_wind,on=["dim_cond_wind_id","Wind_Chill(F)","Wind_Speed(mph)","Precipitation(in)"],how="left")


    fact_final_columns=fact_accidents.columns.drop(['Source',
       'Wind_Direction', 'Wind_Chill(F)', 'Wind_Speed(mph)',
       'Precipitation(in)','dim_cond_wind_id'])
    fact_accidents=fact_accidents[fact_final_columns]



    return fact_accidents


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
