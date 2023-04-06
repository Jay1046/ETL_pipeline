from sqlalchemy import create_engine 

# cursor loader
def rdb_cursor_loader(db_connector, _query_list, _result):
    
    with db_connector as connected:
        cur = connected.conn.cursor()
        
        for _idx, _query in enumerate(_query_list['create'].values()):
            cur.executemany(_query, _result[_idx])
            connected.conn.commit()
            
    return print('데이터 이행이 완료 되었습니다.')



# pandas loader
def rdb_pandas_loader(db_connector, _query_list, _name, _result):
    
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'\
        .format(\
            user = db_connector.user
            , password = db_connector.password
            , host = db_connector.host
            , port = db_connector.port
            , database = db_connector.database)
            )
    
    for _idx, _key in enumerate(_query_list['read'].keys()):
        _result[_idx].to_sql(_key+f'{_name}', engine, if_exists='append', index=False)

    return print("Pandas로 추출한 데이터 이행이 완료되었습니다.")



# pandas custom table loader
def rdb_pandas_loader_custom_table(db_connector,_query_list, _result):
    
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'\
        .format(\
            user = db_connector.user
            , password = db_connector.password
            , host = db_connector.host
            , port = db_connector.port
            , database = db_connector.database)
            )
    
    keys = ''
    for key in _query_list['read'].keys():
        keys += key +'_'
    
    _result.to_sql(f'{keys}', engine, if_exists='append', index=False)
    
    print("Pandas custom table 이행이 완료되었습니다")
    
# ddb loader
def ddb_cursor_loader(db_connector, _query_list, _result):
    
    with db_connector as connected:
        
        for _idx, _dict in enumerate(zip(_query_list['read'].items())):
            _coll = connected.conn.get_collection(_dict[0][0])
            _coll.insert_many(_result[_idx])
            
        print('MongoDB 데이터 이행이 완료되었습니다')
        
# ddb to rdb(pandas)
def rdb_pandas_loader_custom_table_2(db_connector, _name, _result):
    
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'\
        .format(\
            user = db_connector.user
            , password = db_connector.password
            , host = db_connector.host
            , port = db_connector.port
            , database = db_connector.database)
            )
    
    
    _result.to_sql(_name, engine, if_exists='append', index=False)

    return print("Pandas로 추출한 Custom Table 이행이 완료되었습니다.")

# rdb to ddb
def ddb_pandas_loader(db_connector, _result):    
    with db_connector as connected:
        _coll = connected.conn.get_collection('actor')
        _coll.insert_many(_result)
        
        print('MongoDB pandas 데이터 이행이 완료되었습니다')