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
        

        print '*'*10
        
        
        a = get_avg_distance(day1, now_x1, day2, now_x2, length)
        
        print a
        
        oc.subplot(3, 3, i+5)
        oc.plot([j for j in range(length)], [j[1] for j in day1[now_x1:now_x1+length]], "-",\
                [j for j in range(length)], [j[1] for j in day2[now_x2:now_x2+length]], "-")
                
        print "day1["+str(x1)+"+"+str(i)+"] cmp to day2["+str(x2)+"]"        
        oc.title("offset="+str(i)+" distance="+str(a))
        oc.legend('day1','day2')
        print '*'*10
    
def get_distance_test(path_1, path_2):
    day1 = get_one_day_group_by_time(path_1)
    #day2 = get_one_day_group_by_time(path_2)
    day2 = [(730, 3946), (745, 3780), (800, 3986), (815, 3673), (830, 3296), (845, 2925), (900, 2362), (915, 2166), (930, 1591), (945, 1554), (1000, 1224), (1015, 945), (1030, 969), (1045, 759), (1100, 721), (1115, 879), (1130, 680), (1145, 576), (1200, 728), (1215, 831), (1230, 588), (1245, 582), (1300, 735), (1315, 908), (1330, 527), (1345, 670), (1400, 648), (1415, 858), (1430, 531), (1445, 706), (1500, 604), (1515, 680), (1530, 454), (1545, 676), (1600, 635), (1615, 600), (1630, 488)]
    #print day1
    
    try_match(day1, day2)
    
    
if __name__ == "__main__":
    get_distance_test(data_dir_path+r"v0/Addison Road/1/2014-10-6",\
                      data_dir_path+r"v0/Addison Road/1/2014-10-13"\
                      )
    raw_input()
