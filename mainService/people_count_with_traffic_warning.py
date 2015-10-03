import rpyc
stream_handler = rpyc.connect("localhost", 18862)




import datetime
import time

import lib.getTwinedDay
import lib.getDistance


sc = None



result = {}
update_lapse = 60

def function_to_get_peak(a, b):
    if a[1] > b[1]:
        return a
    else:
        return b
        
#today = datetime.datetime.now()
today = datetime.datetime(2014, 10, 15)

def judge_yellow(count, peak, co_count):
    if peak != 0:
        p1 = float(count)/peak
    else:
        p1 = 0.4
        
    if co_count != 0:
        p2 = float(count)/co_count
    else:
        p2 = 1
        
        
    if p1 >= 0.5:
        return True
    elif p1 >= 0.3 and p2 >= 1.2:
        return True

def judge_orange(count, peak, co_count):
    if peak != 0:
        p1 = float(count)/peak
    else:
        p1 = 0.4
        
    if co_count != 0:
        p2 = float(count)/co_count
    else:
        p2 = 1
        
    if p1 >= 0.7:
        return True
    elif p1 >= 0.5 and p2 >= 1.2:
        return True


def judge_red(count, peak, co_count):
    if peak != 0:
        p1 = float(count)/peak
    else:
        p1 = 0.4
        
    if co_count != 0:
        p2 = float(count)/co_count
    else:
        p2 = 1
        
    
    if p1 >= 0.8:
        return True
    elif p1 >= 0.7 and p2 >= 1.2:
        return True

def get_people_count_with_traffic_warning(_sc):
    global sc
    
    if result == {} or time.time() > result['update_time']+update_lapse:
        sc = _sc
        raw_count_dict = stream_handler.root.get_ticket_mechine_data_dict()
        for i in raw_count_dict.iteritems():
            station_name = i[0]
            count_dict = i[1]
            count_list = sorted(list(count_dict.iteritems()), key=lambda x:x[0])
            
            most_recent_record = list(count_list[-1])
            most_recent_count = count_list[-1][1]
            most_recent_time = datetime.datetime(year=2000, month=1, day=1, hour=count_list[-1][0]/100, minute=count_list[-1][0]%100)
            
            print 'count_list', count_list 
            print 'most_recent_record', most_recent_record
            print 'most_recent_time', most_recent_time
            
            result[station_name] = {'most_recent_record':most_recent_record}
            
            twined_day_result = lib.getTwinedDay.get_twined_day(today, station_name, sc)
            
            print 'twined_day_result', twined_day_result
            
            if twined_day_result[0][0] != None:
                twined_day_path = twined_day_result[0][0]
                twined_day_offset = twined_day_result[0][1]
            else:
                twined_day_result[1] = sorted(twined_day_result[1], key = lambda x: datetime.datetime(*[int(i) for i in x.split('/')[-1].split('-')]))
                twined_day_path = twined_day_result[1][-1]
                twined_day_offset = 0
            
            
            data = lib.getDistance.get_one_day_group_by_time(twined_day_path, sc)
            
            adjusted_most_recent_time = most_recent_time-twined_day_offset*datetime.timedelta(minutes=15)
            adjusted_most_recent_int_time = adjusted_most_recent_time.hour*100+adjusted_most_recent_time.minute
            
            print 'twined_day_result', twined_day_result
            print 'adjusted_most_recent_time', adjusted_most_recent_time
            print 'adjusted_most_recent_int_time', adjusted_most_recent_int_time
            
            peak_record = reduce(function_to_get_peak, data)
            
            corresponding_record = filter(lambda x: x[0] == adjusted_most_recent_int_time, data)
            if corresponding_record == []:
                corresponding_record = [0, most_recent_count]
            else:
                corresponding_record = corresponding_record[0]
            
            result[station_name]['peak_record'] = [peak_record]
            result[station_name]['corresponding_record'] = corresponding_record
            
            flag = 0
            if judge_red(most_recent_count, peak_record[1], corresponding_record[1]):
                flag = 3
            elif judge_orange(most_recent_count, peak_record[1], corresponding_record[1]):
                flag = 2
            elif judge_yellow(most_recent_count, peak_record[1], corresponding_record[1]):
                flag = 1
            
            result[station_name]['flag'] = flag
            
        result['update_time'] = time.time()
    
    return result
