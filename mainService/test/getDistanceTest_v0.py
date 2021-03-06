from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

data_dir_path = '/home/sklaw/Desktop/experiment/spark/ex/3/v1/data/'

conf = SparkConf().setMaster("localhost")
sc = SparkContext("local[*]", "data_transformer")
import oct2py
oc = oct2py.Oct2Py()

import math

def pre_process_2(line):
    global mapping
    elem_list = line.split(",")
    return ((int(elem_list[1])), (int(elem_list[2])))

def get_one_day_group_by_time(path):
    file_data = sc.textFile(path)
    data = file_data.map(pre_process_2).reduceByKey(lambda a,b: a+b).collect()
    data = sorted(data, key=lambda x: x[0])
    return data

def get_avg_distance(day1, x1, day2, x2, length):
    result = 0
    i = 0
    while (i < length):
        result += math.fabs(day1[x1][1] - day2[x2][1])
        x1 += 1
        x2 += 1
        i += 1
    return float(result)/length

def try_match(day1, day2):
    x1 = 0
    x2 = [idx for idx, val in enumerate(day2) if val[0]==day1[x1][0]]
    if x2 == []:
        x2 = 0
        x1 = [idx for idx, val in enumerate(day1) if val[0]==day2[x2][0]][0]
    else:
        x2 = x2[0]
    
    
    offset_list = [-4, -3, -2, -1, 0, 1, 2, 3, 4]
    
    print "x1:"+str(x1)+" x2:"+str(x2)
    #day1[x1+i] cmp to day2[x2]
    oc.figure()
    for i in offset_list:
        if (i < 0):
            now_x1 = x1
            now_x2 = x2-i
        else:
            now_x1 = x1+i
            now_x2 = x2
        
    
        l1 = len(day1) - now_x1
        l2 = len(day2) - now_x2
    
        if l1 < l2:
            length = l1
        else:
            length = l2
        

        #print '*'*10
        
        
        a = get_avg_distance(day1, now_x1, day2, now_x2, length)
        
        #print a
        
        oc.subplot(3, 3, i+5)
        oc.plot([j for j in range(length)], [j[1] for j in day1[now_x1:now_x1+length]], "-",\
                [j for j in range(length)], [j[1] for j in day2[now_x2:now_x2+length]], "-")
                
        print "day1["+str(x1)+"+"+str(i)+"] cmp to day2["+str(x2)+"]"        
        oc.title("offset="+str(i)+" distance="+str(a))
        oc.legend('day1','day2')
        #print '*'*10
    
def get_distance_test(path_1, path_2):
    day1 = get_one_day_group_by_time(path_1)
    day2 = get_one_day_group_by_time(path_2)
    
    print day1
    
    try_match(day1, day2)
    
    
if __name__ == "__main__":
    get_distance_test(data_dir_path+r"v0/Addison Road/1/2014-10-6",\
                      data_dir_path+r"v0/Addison Road/1/2014-10-13"\
                      )
    raw_input()
