import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 



import lib.BFS_for_metro_stations

from pymongo import MongoClient

dbtalk = MongoClient("localhost:27017")
station_names = dbtalk.IBM_contest.station_names

routes_similar_to_google_map = dbtalk.IBM_contest.routes_similar_to_google_map
routes_similar_to_google_map.drop()

def analyse_record(record):
    routes = {}
    for station in record.keys():
    
        now_route = []

        now_station = station
        now_platform = None
        
        previous_station_depart_platform = None
        previous_station = None
        
        
        

        
        
        while True:
            if now_platform == None:
                for i in record[now_station]['info_by_platforms'].keys():
                    if now_platform == None:
                        now_platform = i
                    elif record[now_station]['info_by_platforms'][i]['cost'] < record[now_station]['info_by_platforms'][now_platform]['cost']:
                        now_platform = i
            
            now_cost = record[now_station]['info_by_platforms'][now_platform]['cost']
            
            previous_station_depart_platform = record[now_station]['info_by_platforms'][now_platform]['depart_platform_of_previous_station']
            previous_station = record[now_station]['info_by_platforms'][now_platform]['previous_station']
            
            now_route.append([previous_station_depart_platform, now_station, now_platform, now_cost])
            
            if previous_station == '':
                break;
                
            now_station = previous_station
            now_platform = previous_station_depart_platform
    
        routes[station] = now_route[::-1]
    return routes       
                

def print_routes(routes):        
    for i in routes.keys():
        i = routes[i]
    
        route_by_line = []
        now_line = {}
        for j in i:
            if j[0] != j[2]:
                if j[0] == '':
                    now_line = {'line':j[2], 'subroute':[]}
                else:
                    now_line['subroute'].append([j[1], j[3]-lib.BFS_for_metro_stations.get_possible_interchange_time(j[1][0], j[0], j[2])])
                    route_by_line.append(now_line)
                    now_line = {'line':j[2], 'subroute':[]}
            now_line['subroute'].append([j[1], j[3]])
        route_by_line.append(now_line)
        
        print ' '
        print ' '
        print '-'*20
        print route_by_line[0]['subroute'][0], '->', route_by_line[-1]['subroute'][-1]

        
        routes_similar_to_google_map.insert({'enter_station': route_by_line[0]['subroute'][0][0], 'exit_station': route_by_line[-1]['subroute'][-1][0], 'route_by_line': route_by_line})
        
        for j in route_by_line:
            print '*'*10
            print j['line'],j['subroute']
            
interchange_time = 8
def get_possible_interchange_time(station, arrived_platform, depart_platform):
    if arrived_platform != depart_platform:
        return interchange_time
    else:
        return 0

if __name__ == "__main__":
    stations = station_names.find()[0]['data']
    for station in stations:
        record = lib.BFS_for_metro_stations.BFS_runner(station, get_possible_interchange_time)
        if record != {}:
            routes = analyse_record(record)
            print_routes(routes)
            
        
