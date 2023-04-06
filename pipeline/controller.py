from db.connector import DBConnector
from db.queries_rdb import queries_rdb, queries_job1, queries_job2
from db.queries_ddb import queries_ddb
from pipeline import extract, transform, load
from settings import DB_SETTINGS

# rdb to rdb
def etl_1(_yyyymm):
    print('start_etl_1')
    
    result = extract.rdb_cursor_extractor(
        db_connector = DBConnector(**DB_SETTINGS['source_db_localhost_rdb']), 
        _query_list = queries_rdb
        )

    load.rdb_cursor_loader(db_connector = DBConnector(**DB_SETTINGS['target_db_localhost_rdb']),
                    _query_list = queries_rdb,
                    _result = result
                    )
    
    print('end_etl_1')
    

# rdb to rdb using pandas
def etl_2(_yyyymm):
    print('start_etl_2')
    print(_yyyymm)
    
    result = extract.rdb_pandas_extractor(
        db_connector = DBConnector(**DB_SETTINGS['source_db_localhost_rdb']),
        _query_list=queries_job2,
        param= {'batch_month' : _yyyymm}
    )
    
    load.rdb_pandas_loader(
        db_connector= DBConnector(**DB_SETTINGS['target_db_localhost_rdb']),
        _query_list=queries_job2,
        _name = '',
        _result = result
    )
    print('end_etl_2')
    
# rdb to rdb by transform
def etl_3(_yyyymm):
    print('start_etl_3')
    print(_yyyymm)
    
    result = extract.rdb_pandas_extractor(
        db_connector = DBConnector(**DB_SETTINGS['source_db_localhost_rdb']),
        _query_list=queries_job2,
        param= {'batch_month' : _yyyymm}
    )
    
    result_transformed = transform.transformer(result)
    
    load.rdb_pandas_loader_custom_table(
        db_connector= DBConnector(**DB_SETTINGS['target_db_localhost_rdb']),
        _query_list= queries_job2,
        _result = result_transformed
        )
    print('end_etl_3')
    
# ddb to ddb
def etl_4(_yyyymm):
    print('start_etl_4')
    print(_yyyymm)
    
    result = extract.ddb_cursor_extractor(
        db_connector = DBConnector(**DB_SETTINGS['source_db_localhost_ddb']),
        _query_list= queries_ddb)
    
    load.ddb_cursor_loader(
        db_connector = DBConnector(**DB_SETTINGS['target_db_localhost_ddb']),
        _query_list = queries_ddb,
        _result = result
    )
    
    print('end_etl_4')
    
# ddb to rdb
def etl_0():
    print('start_etl_0')
    
    result = extract.ddb_cursor_extractor(
        db_connector = DBConnector(**DB_SETTINGS['source_db_localhost_ddb']),
        _query_list= queries_ddb
        )
    
    result = transform.ddb_to_rdb(result[0])
    
    load.rdb_pandas_loader_custom_table_2(
        db_connector=DBConnector(**DB_SETTINGS['target_db_localhost_rdb']),
        _name = 'bk_list',
        _result=result
    )
    print('end_etl_0')
    
# rdb(pandas) to ddb - only for table 'actor'
def etl_5():
    print('start_etl_5')
    
    result = extract.rdb_pandas_extractor(
        db_connector = DBConnector(**DB_SETTINGS['source_db_localhost_rdb']),
        _query_list=queries_rdb
        )
    
    result_transformed = transform.rdb_to_ddb(result[0])
    
    load.ddb_pandas_loader(
        db_connector = DBConnector(**DB_SETTINGS['target_db_localhost_ddb']),
        _result = result_transformed
    )
    print('end_etl_5')