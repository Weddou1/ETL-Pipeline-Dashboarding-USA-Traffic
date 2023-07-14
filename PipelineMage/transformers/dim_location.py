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
    
    dim_location=get_unique_values(df,['Street','City','Zipcode', 'State','County', 'Country','Timezone'])
    get_id_column(dim_location,"dim_location_id")

    return dim_location


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
