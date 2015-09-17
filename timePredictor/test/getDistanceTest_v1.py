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
    day2 = get_one_day_group_by_time(path_2)
    
    #2014 10 6 Addison Road
    #[(0, 23), (15, 43), (30, 18), (45, 30), (100, 24), (115, 17), (130, 7), (145, 11), (200, 9), (215, 11), (245, 2), (315, 2), (430, 7), (445, 1132), (500, 574), (515, 1208), (530, 1550), (545, 1399), (600, 2312), (615, 2254), (630, 2761), (645, 2709), (700, 3270), (715, 3984), (730, 3750), (745, 4114), (800, 3635), (815, 3428), (830, 2856), (845, 2405), (900, 2183), (915, 1570), (930, 1583), (945, 1168), (1000, 930), (1015, 947), (1030, 761), (1045, 729), (1100, 881), (1115, 690), (1130, 566), (1145, 723), (1200, 845), (1215, 619), (1230, 593), (1245, 727), (1300, 918), (1315, 518), (1330, 659), (1345, 613), (1400, 843), (1415, 510), (1430, 701), (1445, 616), (1500, 680), (1515, 451), (1530, 704), (1545, 624), (1600, 618), (1615, 475), (1630, 577), (1645, 708), (1700, 468), (1715, 478), (1730, 462), (1745, 426), (1800, 380), (1815, 396), (1830, 371), (1845, 417), (1900, 330), (1915, 376), (1930, 346), (1945, 305), (2000, 273), (2015, 242), (2030, 324), (2045, 280), (2100, 217), (2115, 313), (2130, 230), (2145, 218), (2200, 226), (2215, 313), (2230, 132), (2245, 157), (2300, 137), (2315, 151), (2330, 46), (2345, 41)]

    
    print day1
    
    try_match(day1, day2)
    
    
if __name__ == "__main__":
    get_distance_test(data_dir_path+r"v0/Addison Road/1/2014-10-6",\
                      data_dir_path+r"v0/Addison Road/1/2014-10-13"\
                      )
    raw_input()
