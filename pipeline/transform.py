import pandas as pd

def transformer(result):
    joined_1 = result[2].join(result[0].set_index('actor_id'), on='actor_id',  rsuffix='_actor')
    pdf = joined_1.join(result[1].set_index('film_id'), on='film_id', rsuffix='_film')
    
    return pdf

def ddb_to_rdb(_result):
    return pd.DataFrame(_result)

def rdb_to_ddb(_result):
    _to_dict = _result.to_dict(orient='index')
    
    result = [_value for _idx, _value in _to_dict.items()]
    return result