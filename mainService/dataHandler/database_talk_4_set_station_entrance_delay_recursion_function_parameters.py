from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import os
package_directory = os.path.dirname(os.path.abspath(__file__))
data_dir_path = package_directory+'/../../../data/'

conf = SparkConf().setMaster("localhost")
sc = SparkContext("local[*]", "data_transformer")
sc.setLogLevel("WARN")

import glob

import numpy
from numpy import matrix
from numpy import linalg

from pymongo import MongoClient

dbtalk = MongoClient("localhost:27017")

entrance_delay_function_parameter = dbtalk.IBM_contest.entrance_delay_function_parameter
#entrance_delay_function_parameter.drop()

station_names = dbtalk.IBM_contest.station_names
stations = station_names.find()[0]['data']

station_destinations_by_directions = dbtalk.IBM_contest.station_destinations_by_directions
routes_similar_to_google_map = dbtalk.IBM_contest.routes_similar_to_google_map



#let time be the key
def pre_process_1(line):
    global mapping
    elem_list = line.split(",")
    return ( ( int(elem_list[1]) ), ( elem_list[0], int(elem_list[2]), float(elem_list[3]) ) )




def map_anlalyser_gen(_station_name, _destinations_by_directions, _pure_routes_pairs):
    #item = (time, [ext_station, rider_sum, avg_ridership_time])
    def func(item):
        #if item[0] != 800:
        #    return;       
        time = item[0]
        data = list(item[1])
        
        station_name = str(_station_name)
        destinations_by_directions = list(_destinations_by_directions)
        pure_routes_pairs = dict(_pure_routes_pairs)
        
        C1_delay_pair_dict = {}
        for i in destinations_by_directions:
            direction = i[0]
            destinations = i[1]
            C1_delay_pair_dict[direction] = [0,0]
            
            
            #C1 filter here
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
            delay = reduce(lambda a,b: (a+b)/2, delay)
            
            C1_delay_pair_dict[direction][1] = delay
        return (time,C1_delay_pair_dict)     
    return func
    
def get_pure_routes_pairs(routes):
    tmp_dict = {}
    for i in routes:
        tmp_dict[i['exit_station']] = i['route_by_line'][-1]['subroute'][-1][1]
    return tmp_dict


def analyse_one_day(station_name, path, record_dict):
    #print path
    tmp_dict = {}
    
    station_info = station_destinations_by_directions.find_one({"station_name":station_name})
    if station_info == None:
        return tmp_dict
    destinations_by_directions = station_info['destinations_by_directions']
    routes = list(routes_similar_to_google_map.find({"$and":[{'enter_station': station_name}, {'route_by_line':{"$size":1}}]}))
    if routes == []:
        return tmp_dict
    
    #pairs --> (exit_station, standard_time)
    pure_routes_pairs = get_pure_routes_pairs(routes)
    
    file_data = sc.textFile(path)
    func = map_anlalyser_gen(station_name, destinations_by_directions, pure_routes_pairs)
    C1_delay_pairs_of_one_day = file_data.map(pre_process_1).groupByKey().map(func).collect()

    for i in C1_delay_pairs_of_one_day:
        if i == None:
            continue
        tmp_dict[i[0]] = i[1]
    return tmp_dict


#VERY IMPORTANT: y = p1+p2*(numpy.log(x+1)), we will make a module for the recursion function in /lib
def get_parameters(points):
    tmp_dict = {}
    for i in points:
        if i[0] not in tmp_dict.keys():
            tmp_dict[i[0]] = float(i[1])
        else:
            tmp_dict[i[0]] = (tmp_dict[i[0]]+i[1])/2
        
    tmp_dict[0] = min([i[1] for i in points])
        
    points = tmp_dict.iteritems()

    points = sorted(points, key=lambda x: x[0])
        
    if len(points) < 2:
        return None
        
    vec_y = []
    for i in points:
        vec_y.append([i[1]])
    vec_y = matrix(vec_y)     
    matrix_x = matrix( [ [1, numpy.log(100*i[0]+1)] for i in points ] )   
    matrix_target = ((matrix_x.T)*(matrix_x)).I*(matrix_x.T)*vec_y

    return [matrix_target[0, 0], matrix_target[1, 0]]


def set_parameter_with_station_direction_time_wday(record_dict, station_name, day_type):
    query_amount = 0
    row_info = []
    fat_x = []
    fat_y = []
    bag_info = []
    for time in sorted(record_dict.keys()):
        for direction in record_dict[time].keys():
            '''
            #GPU part
            points = record_dict[time][direction]
            tmp_dict = {}
            for i in points:
                if i[0] not in tmp_dict.keys():
                    tmp_dict[i[0]] = float(i[1])
                else:
                    tmp_dict[i[0]] = (tmp_dict[i[0]]+i[1])/2
                    
                    
            tmp_dict[0] = min([i[1] for i in points])
            points = list(tmp_dict.iteritems())
            if len(points) < 2:
                continue
            points = sorted(points, key=lambda x: x[0])
            
            vec_y = [i[1] for i in points]
            matrix_x = []
            for i in points:
                matrix_x.append(1)
                matrix_x.append(numpy.log(100*i[0]+1))
            
            print '-'*20
            #print matrix_x
            #print vec_y
            fat_x += matrix_x
            fat_y += vec_y
            query_amount += 1
            row_info.append(len(matrix_x))
            bag_info.append((station_name, time, direction))
            ''' 
                    
            #origin part
            parameters = get_parameters(record_dict[time][direction])
            if parameters == None:
                continue
            print station_name, time, direction, day_type, parameters
            #entrance_delay_function_parameter.insert({"station_name":station_name, "time":time, "direction":direction, "day_type":day_type, "parameters":parameters})
            

    '''
    print fat_x
    print '-'*20
    print fat_y
    print '*'*20
    print query_amount
    print '+'*20
    print row_info
    print '&'*20
    print bag_info
    '''


def show_trends(station_name):
    print station_name
    day_types = [1, 6, 7, 8]

    for wday in day_types:
        if wday == 6:
            path = data_dir_path+'v0/'+station_name+'/6/*'
        elif wday == 7:
            path = data_dir_path+'v0/'+station_name+'/7/*'
        elif wday == 8:
            path = data_dir_path+'v0/'+station_name+'/holiday/*'
        else:
            path = data_dir_path+'v0/'+station_name+'/[1-5]/*'
        file_paths = glob.glob(path)
    
        record_dict = {}
        for path in file_paths:
            tmp_dict = analyse_one_day(station_name, path, record_dict)
            
            
            for time in tmp_dict.keys():
                if time not in record_dict.keys():
                    record_dict[time] = {}
                
                for direction in tmp_dict[time].keys():
                    if direction not in record_dict[time].keys():
                        record_dict[time][direction] = []
                    record_dict[time][direction].append(tmp_dict[time][direction])

        set_parameter_with_station_direction_time_wday(record_dict, station_name, wday)
        break;



if __name__ == "__main__":
    for station in stations:
        show_trends(station);
        break;
        
