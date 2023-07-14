if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def get_unique_values(df,columns):
    return df[columns].drop_duplicates().reset_index(drop=True)

def get_id_column(df,column_name):
    return df.insert(0, column_name, range(1, len(df) + 1))

@transformer
def transform(df, *args, **kwargs):
    
    dim_volatile_road_attribute=get_unique_values(df,['Amenity',
       'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
       'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal',
       'Turning_Loop'])
    dim_volatile_road_attribute=dim_volatile_road_attribute.astype(str).replace({'True': '1', 'False': '0','nan': 'N'})
    binary_road_attribute=dim_volatile_road_attribute.apply(lambda row: ''.join(row.astype(str)), axis=1)
    dim_volatile_road_attribute=get_unique_values(df,['Amenity',
       'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
       'Roundabout', 'Station', 'Stop', 'Traffic_Calming', 'Traffic_Signal',
       'Turning_Loop'])
    dim_volatile_road_attribute["binary_road_attribute"]=binary_road_attribute

    return dim_volatile_road_attribute


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
