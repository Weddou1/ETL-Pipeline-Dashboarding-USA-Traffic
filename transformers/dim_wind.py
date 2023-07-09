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
def transform(data, data_2):

    dim_wind=get_unique_values(data,['Wind_Direction','Wind_Chill(F)',
       'Wind_Speed(mph)', 'Precipitation(in)'])

    dim_wind=dim_wind.merge(data_2, on='Wind_Direction', how='left').iloc[:,1:]
    dim_wind=move_column_to_first_position(dim_wind,3)
    get_id_column(dim_wind,"dim_wind_id")

    return dim_wind


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
