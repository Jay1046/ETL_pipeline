DB_SETTINGS = {
    'source_db_localhost_rdb': {
        'host' : "127.0.0.1",
        'port' : "5432",
        'user' : "postgres",
        'password' : "qwe123",
        'database' : "dvdlental",
        'location' : "localhost_source",
        'engine' : "postgre"
    },
    'source_db_localhost_ddb' : {
        'host' : "127.0.0.1",
        'port' : 27017,
        'user' : "root",
        'password' : "qwe123",
        'database' : "yes24",
        'location' : "localhost_source",
        'engine' : "mongodb"
    },
    'target_db_localhost_rdb' : {
        'host' : "127.0.0.1",
        'port' : "5432",
        'user' : "postgres",
        'password' : "qwe123",
        'database' : "temp0",
        'location' : "localhost_target",
        'engine' : "postgre"
    },
    'target_db_localhost_ddb' : {
        'host' : "127.0.0.1",
        'port' : 27017,
        'user' : "root",
        'password' : "qwe123",
        'database' : "temp0",
        'location' : "localhost_target",
        'engine' : "mongodb"
    },
}