if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def get_unique_values(df,columns):
    return df[columns].drop_duplicates().reset_index(drop=True)

def get_id_column(df,column_name):
    return df.insert(0, column_name, range(1, len(df) + 1))
def move_column_to_first_position(df, column_index):
    column = df.columns[column_index]
    df_columns = df.columns.tolist()
    df_columns.insert(0, df_columns.pop(column_index))
    df = df[df_columns]
    df = df.rename(columns={column: column + "_moved"})
    df = df.rename(columns={df.columns[0]: column})
    return df

@transformer
def transform(data, data_2, data_3):
    
    dim_accident_location=get_unique_values(data,[ 'Start_Lat',
    'Start_Lng', 'Street','City','Zipcode', 'State','County', 'Country','Timezone',
    'Airport_Code'])
    dim_accident_location=dim_accident_location.merge(data_2, on='Airport_Code', how='left').merge(data_3,on=['Street','City','Zipcode', 'State','County', 'Country','Timezone'],how='left').iloc[:,[0,1,-1,-2]]

    return dim_accident_location


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
