queries_rdb = {
    'read': {
        'actor': f'''
            SELECT * 
            FROM actor
            ;
        ''',
        'film': f'''
            SELECT * 
            FROM film
            ;
        ''',
        'film_actor': f'''
            SELECT *
            FROM film_actor
            ;
        '''
        },
    'create': {
        'actor': '''
            INSERT INTO actor VALUES (%s, %s, %s, %s)
            ;
        ''',
        'film': '''
            INSERT INTO film VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ;
        '''
        }
    }

queries_job1 = {
    'read': {
        'actor': f'''
            SELECT * 
            FROM actor
            ;
        ''',
        'film': f'''
            SELECT * 
            FROM film
            ;
        ''',
        'film_actor': f'''
            SELECT *
            FROM film_actor
            ;
        '''
        }
    }

queries_job2 = {
    'read': {
        'actor': '''
            SELECT '{batch_month}' AS YYYYMM, * 
            FROM actor 
            ;
        ''',
        'film': '''
            SELECT '{batch_month}' AS YYYYMM, * 
            FROM film 
            ;
        ''',
        'film_actor': '''
            SELECT '{batch_month}' AS YYYYMM, *
            FROM film_actor
            ;
        '''
        }
    }

# print(queries_rdb['read']['actor'])

# queries_ddb = {
#     'read': {
#         'bk_list': '''
#             {"영역":{"$eq":"기본서"}}
#         ''',
#         # 'e_eng_2022_11' : '''
#         #     {}
#         # '''
#     }
# }
# print(queries_ddb['read']['bk_list'])