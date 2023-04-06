from pipeline import controller
import settings
from datetime import datetime, timedelta
import click
import sys

# main f
@click.command()
@click.option('-m', '--custom-batch-month', type= click.STRING, default='', help='배치작업연월')
def start_batch(custom_batch_month):
    print('input:', custom_batch_month)
    batch_month = _get_batch_month(custom_batch_month)
    print("get_batch_month : ", batch_month)
    
    if not batch_month:
        print('batch_month is None')
        sys.exit(1)
    try:
        # etl_1(batch_month) # etl 실행
        # etl_2(batch_month)
        # etl_3(batch_month)
        # etl_4(batch_month)
        # controller.etl_3(batch_month)
        controller.etl_5()
    except Exception as e:
        print(e)
        sys.exit(1)
    sys.exit(0)
    
    
# batch month get f
def _get_batch_month(_custom_batch_month):
    if _custom_batch_month: # None이 아닌 값이 들어온다면 유효성 검사를 통해 return 함
        print('custom_batch > batch_month : ', _custom_batch_month)
        return _check_valid_month(_custom_batch_month)
    
    # None값이 들어온다면 해당 날짜의 전월을 계산해서 return함
    first_day = datetime.today().replace(day = 1) # 오늘의 날짜를 replace를 사용하여 오늘이 몇일이든 1일로 변경
    batch_month = first_day - timedelta(days = 1) # timedelta는 날짜의 연산을 할수있게 해준다
    return batch_month.strftime('%Y%m') # strftime은 해당 날을 str 타입으로 변경해준다
    
    
# date 유효성 체크
def _check_valid_month(str_yyyymm):
    try:
        # print(str_yyyymm)
        datetime.strptime(str_yyyymm, '%Y%m') #strptime : str값을 가져와서 해당 format으로 변환
        return str_yyyymm
    except Exception as e:
        return None
    

    
if __name__ == '__main__':
    print('start_batch_job')
    start_batch()
    print('end_batch_job')
    
    
