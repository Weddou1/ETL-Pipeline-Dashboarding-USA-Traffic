if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
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

@custom
def transform_custom(data, *args, **kwargs):
    """
    Args:
        data: The output from the upstream parent block (if applicable)
        args: The output from any additional upstream blocks

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here

    return data.iloc[:,0:]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
