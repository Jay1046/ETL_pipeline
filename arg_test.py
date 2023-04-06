import click

@click.command()
@click.option('-m', '--custom-batch-month', type= click.STRING, default='', help='배치작업연월')
def start_batch(custom_batch_month):
    
    _yyyymm = _check_valid_month(custom_batch_month)
    
    print(custom_batch_month)
    print(custom_batch_month)
    
    
def _check_valid_month(str_yyyymm):
    try:
        print(str_yyyymm)
        datetime.strptime(str_yyyymm, '%Y%m') #strptime : str값을 가져와서 해당 format으로 변환
        return str_yyyymm
    except Exception as e:
        return None
    

    
if __name__ == '__main__':
    start_batch()