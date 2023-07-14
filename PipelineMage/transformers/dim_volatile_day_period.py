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
    
    dim_volatile_day_period=get_unique_values(df,['Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight',
       'Astronomical_Twilight']).astype(str).replace({'Day': '1', 'Night': '0','nan': 'N'})

    binary_day=dim_volatile_day_period.apply(lambda row: ''.join(row.astype(str)), axis=1)

    dim_volatile_day_period=get_unique_values(df,['Sunrise_Sunset', 'Civil_Twilight', 'Nautical_Twilight',
       'Astronomical_Twilight'])
    dim_volatile_day_period["binary_day"]=binary_day

    return dim_volatile_day_period


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
