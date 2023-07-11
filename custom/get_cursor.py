if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@custom
def transform_custom(pgconn, *args, **kwargs):
    
    pgcursor = pgconn.iloc[0,0].cursor()

    

    return pd.Dataframe({"Value" :pgcursor})


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
