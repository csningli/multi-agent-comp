
import datetime

def get_current_time() :
    return datetime.datetime.now().strftime("%Y/%m/%d_%H:%M:%S.%f")

def get_datetime_stamp() :
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
