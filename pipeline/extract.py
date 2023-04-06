import pandas as pd

def rdb_cursor_extractor(db_connector, _query_list, param=None):
    
    result = []
    with db_connector as connected:
        cur = connected.conn.cursor()
        
        for _key, _value in _query_list['read'].items():
            
            if param != None: # 만일 주어진 batch_month가 있다면 formatting 후 extract
                cur.execute(_value.format(**param)) 
                
            else: # 없다면 그냥 extract
                cur.execute(_value)
                
            result.append(cur.fetchall())
        
    return result


def rdb_pandas_extractor(db_connector, _query_list, param=None):
    
    result = []
    with db_connector as connected:
        con = connected.conn
        for _key, _value in _query_list['read'].items():
            
            if param != None:
                result.append(pd.read_sql_query(_value.format(**param), con))
            else:
                result.append(pd.read_sql_query(_value, con))
            
    return result


from ast import literal_eval

def ddb_cursor_extractor(db_connector, _query_list, param = None):
    
    with db_connector as connected:
    
        result = []
        for _collection, _query in _query_list['read'].items():
                _coll = connected.conn.get_collection(_collection)
                
                # literal_eval은 str을 안에 값인 dict로 만들어준다
                if param != None:
                    _doc = _coll.find(literal_eval(_query.format(**param).strip()))
                else:
                    _doc = _coll.find(literal_eval(_query.strip()))
                
                _row = []
                for row in _doc:
                    row.pop("_id")
                    _row.append(row)
                
                result.append(_row)
            
    return result