from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

data_dir_path = '/home/sklaw/Desktop/experiment/spark/ex/3/v1/data/'

conf = SparkConf().setMaster("localhost")
sc = SparkContext("local[*]", "data_transformer")

sc.setLogLevel("WARN")

import oct2py
oc = oct2py.Oct2Py()

import glob

import numpy
from numpy import matrix
from numpy import linalg

import math

from pymongo import MongoClient
dbtalk = MongoClient("localhost:27017")
station_destinations_by_directions = dbtalk.IBM_contest.station_destinations_by_directions
routes_similar_to_google_map = dbtalk.IBM_contest.routes_similar_to_google_map


def ext_station_to_direction_map_func_gen(destinations_by_directions):
    def func(item):
        ext_station = item[0]
        direction = None
        for i in destinations_by_directions:
            if ext_station in i[1]:
                direction = i[0]
                break;
        
        return ((direction),(item[1]))
    return func
    
    

#format of the RDD:   ext_station, ridership sum
def get_C1(station_name, RDD_of_one_day_at_one_time):
    station_info = station_destinations_by_directions.find_one({"station_name":station_name})
    if station_info == None:
        return None
    destinations_by_directions = station_info['destinations_by_directions']
    return RDD_of_one_day_at_one_time.map(ext_station_to_direction_map_func_gen(destinations_by_directions)).reduceByKey(lambda a,b: a+b).filter(lambda x: x[0] != None).collect()
    
    


def pre_process_1(line):
    global mapping
    elem_list = line.split(",")
    return ( ( int(elem_list[1]) ), ( elem_list[0], int(elem_list[2]), float(elem_list[3]) ) )

def get_direction_by_ext_station(ext_station, destinations_by_directions):
    for i in destinations_by_directions:
        if ext_station in i[1]:
            return i[0]
    return None


def map_anlalyser_gen(station_name, destinations_by_directions, pure_routes_pairs):
    def func(item):
        #if item[0] != 800:
        #    return;       
        time = item[0]
        data = list(item[1])
        
        C1_delay_pair_dict = {}
        

        
        for i in destinations_by_directions:
            direction = i[0]
            destinations = i[1]
            C1_delay_pair_dict[direction] = [0,0]
            
            
            C1 = [j[1] for j in data if j[0] in destinations]
            if C1 == []:
                continue      
            C1 = reduce(lambda a,b: a+b, C1)
            C1_delay_pair_dict[direction][0] = C1
            
            
            
            delay = []
            filtered_pure_routes_ext_stations = [j for j in pure_routes_pairs.keys() if j in destinations]
            for j in data:
                if j[0] in filtered_pure_routes_ext_stations:
                    #print j[0], j[2], pure_routes_pairs[j[0]]
                    #delay.append([j[0], j[2]-pure_routes_pairs[j[0]]])
                    
                    delay.append(j[2]-pure_routes_pairs[j[0]])
            if delay == []:
                continue      
            delay = min(delay)
            
            
            C1_delay_pair_dict[direction][1] = delay
        
        '''
        if time not in record_dict.keys():
            record_dict[time] = {}
        
        for i in C1_delay_pair_dict.keys():
            if i not in record_dict[time].keys():
                record_dict[time][i] = []
            record_dict[time][i].append(C1_delay_pair_dict[i])
        '''
        
        return (time,C1_delay_pair_dict)
                  
    return func

#pure routes means the routes that havn't interchanged lines.
def get_pure_routes(routes):
    l = [(i['exit_station'], i['route_by_line'][-1]['subroute'][-1][1]) for i in routes if len(i['route_by_line']) == 1]
    tmp_dict = {}
    for i in l:
        tmp_dict[i[0]] = i[1]
    return tmp_dict

def analyse_one_day(station_name, path, record_dict):

    print path

    station_info = station_destinations_by_directions.find_one({"station_name":station_name})
    
    if station_info == None:
        return
    
    destinations_by_directions = station_info['destinations_by_directions']
    
    routes = list(routes_similar_to_google_map.find({'enter_station': station_name}))

    

    if routes == []:
        return

    #pairs --> (exit_station, standard_time)
    pure_routes_pairs = get_pure_routes(routes)


    file_data = sc.textFile(path)
    
    func = map_anlalyser_gen(station_name, destinations_by_directions, pure_routes_pairs)
    
    C1_delay_pairs_of_one_day = file_data.map(pre_process_1).groupByKey().map(func).collect()

    tmp_dict = {}
    
    for i in C1_delay_pairs_of_one_day:
        if i == None:
            continue
        
        tmp_dict[i[0]] = i[1] 

    return tmp_dict

def get_rid_of_outliers(points):
    points = sorted(points, key=lambda x: x[1])
    
    print '.'*10
    print len(points)
    print points
    size = len(points)
    
    smallest_half = points[:(size+1)/2]
    largest_half = points[size/2:]
    
    if size%2 == 0:
        lower_fourth = (smallest_half[size/4][1]+smallest_half[size/4-1][1])/2
        upper_fourth = (largest_half[size/4][1]+largest_half[size/4-1][1])/2
    else:
        lower_fourth = smallest_half[size/4][1]
        upper_fourth = largest_half[size/4][1]
    
    box_width = upper_fourth-lower_fourth
    
    print box_width, lower_fourth, upper_fourth
    
    lower_outlier = lower_fourth-1.5*box_width
    upper_outlier = upper_fourth+1.5*box_width
    
    print lower_outlier, upper_outlier
    
    points = [i for i in points if i[1] < upper_outlier and i[1] > lower_outlier]
    
    print len(points)
    
    return points

def view_trend_at_time(data):
    for direction in data.keys():
        points = data[direction]
        
        
        
        
        tmp_dict = {}
        for i in points:
            if i[0] not in tmp_dict.keys():
                tmp_dict[i[0]] = float(i[1])
            else:
                tmp_dict[i[0]] = (tmp_dict[i[0]]+i[1])/2
        
        tmp_dict[0] = min([i[1] for i in points])
        
        points = tmp_dict.iteritems()
        
        
        
        
        
        #points = get_rid_of_outliers(points)
        
        points = sorted(points, key=lambda x: x[0])
        
        if len(points) <= 2:
            continue
        
        vec_y = []
        for i in points:
            vec_y.append([i[1]])
        vec_y = matrix(vec_y)
        
        
        
        matrix_x = matrix( [ [1, numpy.log(100*i[0]+1)] for i in points ] )   
        matrix_target = ((matrix_x.T)*(matrix_x)).I*(matrix_x.T)*vec_y
        x = oc.linspace(0, points[-1][0])
        y = matrix_target[0, 0]+matrix_target[1, 0]*numpy.log(100*x+1)
        
        
        matrix_x = matrix( [ [1, numpy.log(i[0]+1)] for i in points ] )   
        matrix_target = ((matrix_x.T)*(matrix_x)).I*(matrix_x.T)*vec_y
        x2 = oc.linspace(0, points[-1][0])
        y2 = matrix_target[0, 0]+matrix_target[1, 0]*numpy.log(x+1)
            
        
        oc.figure()
        
        #oc.plot([i[0] for i in points], [i[1] for i in points])
        oc.plot([i[0] for i in points], [i[1] for i in points], "x",\
                x,y, "-",\
                x2,y2, "-")
        oc.legend('raw','fast saturation', 'slow saturation')
        
        oc.title(direction)

def view_trend_at_one_day(raw_dict, title):
    raw_list = list(raw_dict.iteritems())
    
    tmp_dict = {}
    for i in raw_list[0][1].keys():
        tmp_dict[i] = []
    
    for i in raw_list:
        for j in i[1].keys():
            tmp_dict[j].append(i[1][j])
    
    for i in tmp_dict.keys():
        direction = i
        points = tmp_dict[i]
        points = sorted(points, key=lambda x: x[0])
        
        
        vec_y = []
        for i in points:
            if i[0] == 0:
                continue
            vec_y.append([i[1]])
        vec_y = matrix(vec_y)
        
        print points
        matrix_x = matrix( [ [1, numpy.log(i[0])] for i in points if i[0] != 0] )
        
        matrix_target = ((matrix_x.T)*(matrix_x)).I*(matrix_x.T)*vec_y
        
        print matrix_target
        
        x = oc.linspace(0, points[-1][0])
        y = matrix_target[0, 0]+(numpy.log(x))*matrix_target[1, 0]
           
        
        oc.figure()
        oc.plot([i[0] for i in points], [i[1] for i in points], "-",\
                x,y, "-")
        oc.title(title+' '+direction)
        
def show_trends(station_name):
    day_types = [1, 6, 7]

    for wday in day_types:
        if wday == 6:
            path = data_dir_path+'v0/'+station_name+'/6/*'
        elif wday == 7:
            path = data_dir_path+'v0/'+station_name+'/7/*'
        #elif wday == 8:
        #    path = data_dir_path+'v0/'+station_name+'/*/*'
        else:
            path = data_dir_path+'v0/'+station_name+'/[1-5]/*'
        file_paths = glob.glob(path)
    
        record_dict = {}
        for path in file_paths:
            tmp_dict = analyse_one_day(station_name, path, record_dict)
            
            #view_trend_at_one_day(tmp_dict, path)
            
            for time in tmp_dict.keys():
                if time not in record_dict.keys():
                    record_dict[time] = {}
                
                for direction in tmp_dict[time].keys():
                    if direction not in record_dict[time].keys():
                        record_dict[time][direction] = []
                    record_dict[time][direction].append(tmp_dict[time][direction])
        view_trend_at_time(record_dict[1100])
        break;

if __name__ == "__main__":
    show_trends('Metro Center');
    raw_input()
