if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import json
import psycopg2


@custom
def transform_custom(*args, **kwargs):
    
    conn = psycopg2.connect("dbname=USAAccidents user=postgres password=admin")

    return json.dumps({"conn" : conn})


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
