from pymongo import MongoClient

dbtalk = MongoClient("localhost:27017")
metro_map = dbtalk.IBM_contest.metro_map

stations = ['White Flint', 'Capitol South', 'Morgan Blvd.', 'Arlington Cemetery', 'Waterfront', 'Vienna', 'Franconia-Springfield', 'Crystal City', "Prince George's Plaza", 'Forest Glen', 'Branch Avenue', 'Grosvenor', 'West Hyattsville', 'Minnesota Avenue', 'Dunn Loring', 'McPherson Square', 'Glenmont', 'Metro Center', 'Shaw-Howard University', 'Benning Road', 'Reagan Washington National Airport', 'Takoma', 'New Carrollton', 'Tenleytown-AU', 'Foggy Bottom', 'Huntington', 'Cleveland Park', 'U Street-Cardozo', 'Naylor Road', 'Brookland', 'Mt. Vernon Square-UDC', 'Farragut North', 'Georgia Avenue-Petworth', 'Gallery Place-Chinatown', 'Stadium-Armory', 'Wiehle', 'Potomac Avenue', 'Braddock Road', 'East Falls Church', 'Wheaton', 'Spring Hill', 'Archives-Navy Memorial', 'Court House', 'Virginia Square-GMU', 'King Street', 'Congress Heights', 'Pentagon City', 'Landover', 'Cheverly', 'Woodley Park-Zoo', 'Farragut West', 'Bethesda', 'Columbia Heights', 'Greenbelt', 'Van Ness-UDC', 'Addison Road', 'Silver Spring', 'Navy Yard', 'Largo Town Center', 'Dupont Circle', 'McLean', 'Tysons Corner', 'Friendship Heights', 'Twinbrook', 'Smithsonian', 'Medical Center', 'Ballston', 'Suitland', 'Rhode Island Avenue', 'Van Dorn Street', 'Federal Center SW', 'Eastern Market', "L'Enfant Plaza", 'Deanwood', 'Rockville', 'College Park-U of MD', 'Capitol Heights', 'Greensboro', 'Union Station', 'Shady Grove', 'Judiciary Square', 'Federal Triangle', 'New York Ave', 'Clarendon', 'Anacostia', 'Southern Avenue', 'West Falls Church', 'Pentagon', 'Rosslyn', 'Fort Totten', 'Eisenhower Avenue']

interchange_time = 5




database_directions = dbtalk.IBM_contest.directions


record = {}

future_nodes = []

def get_possible_interchange_time(station, arrived_platform, depart_platform):
    if arrived_platform != depart_platform:
        return interchange_time
    else:
        return 0

#def try_to_mark(previouse_to_current_line, current_to_next_line, next_node_cost_without_interchange, current_node, next_node):
def try_to_mark(current_platform_arrived, current_platform_to_depart, origin_to_next_ideal_cost, current_node, next_node):
    global record, future_nodes
    
    #read it this way, next, platform arrvied. means the platform arrvied at next node
    next_platform_arrived = current_platform_to_depart

    #to check if we need some extra time spending on current_node, though we've arrived at next_node
    next_node_cost = origin_to_next_ideal_cost + get_possible_interchange_time(current_node, current_platform_arrived, current_platform_to_depart)
    
    mark= False
    
    #now, we try to mark next_node in record.
    
    next_node_info = metro_map.find_one({"station_name":next_node})
    next_departing_platfroms = next_node_info['line']
    
    if next_node not in record.keys():
        record[next_node] = {'info_by_platforms':{}}
        


    for next_per_platform in next_departing_platfroms:
        tmp_cost = next_node_cost+get_possible_interchange_time(next_node, next_platform_arrived, next_per_platform)
        if next_per_platform not in record[next_node]['info_by_platforms'].keys() or tmp_cost < record[next_node]['info_by_platforms'][next_per_platform]['cost']:
            record[next_node]['info_by_platforms'][next_per_platform] = {}
            record[next_node]['info_by_platforms'][next_per_platform]['cost'] = tmp_cost
            record[next_node]['info_by_platforms'][next_per_platform]['previous_station'] = current_node
            record[next_node]['info_by_platforms'][next_per_platform]['depart_platform_of_previous_station'] = current_platform_to_depart
            if next_node not in future_nodes:
                future_nodes.append(next_node)
    
    

def BFS(current_nodes):
    global record, future_nodes
    future_nodes = []
    if len(current_nodes) == 0:
        #print record
        return
    #print current_nodes
    for current_node in current_nodes:
        if current_node not in record:
            record[current_node] = {'info_by_platforms':{}}
            
        current_node_info = metro_map.find_one({"station_name":current_node})
        
        for goto_item in current_node_info['goto']:
            next_node = goto_item[0]
            current_to_next_ideal_cost = goto_item[1]
            platforms_on_current_to_next = goto_item[2]
            
            for current_platform_to_depart in platforms_on_current_to_next:
                if len(record[current_node]['info_by_platforms']) == 0:
                    #this node is the origin
                    origin_to_next_ideal_cost = 0+current_to_next_ideal_cost
                    try_to_mark(None, current_platform_to_depart, origin_to_next_ideal_cost, current_node, next_node)
                else:
                    for current_platform_arrived in record[current_node]['info_by_platforms'].keys():
                        origin_to_next_ideal_cost = record[current_node]['info_by_platforms'][current_platform_to_depart]['cost']+current_to_next_ideal_cost
                        try_to_mark(current_platform_arrived, current_platform_to_depart, origin_to_next_ideal_cost, current_node, next_node)
    current_nodes = future_nodes
    BFS(current_nodes)

def BFS_runner(origin_node):
    origin_node_info = metro_map.find_one({"station_name":origin_node})
    origin_departing_platfroms = origin_node_info['line']
    record[origin_node] = {'info_by_platforms':{}}
    for origin_per_platform in origin_departing_platfroms:
        record[origin_node]['info_by_platforms'][origin_per_platform] = {}
        record[origin_node]['info_by_platforms'][origin_per_platform]['cost'] = 0
        record[origin_node]['info_by_platforms'][origin_per_platform]['previous_station'] = ""
        record[origin_node]['info_by_platforms'][origin_per_platform]['depart_platform_of_previous_station'] = ""
    
    BFS([origin_node])

def analyse_record():
    global record
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
                    now_line = {'line':j[2], 'meat':[]}
                else:
                    now_line['meat'].append([j[1]])
                    route_by_line.append(now_line)
                    now_line = {'line':j[2], 'meat':[]}
            now_line['meat'].append([j[1], j[3]])
        route_by_line.append(now_line)
        
        print ' '
        print ' '
        print '-'*20
        print route_by_line[0]['meat'][0], '->', route_by_line[-1]['meat'][-1]
        for j in route_by_line:
            print '*'*10
            print j['line'],j['meat']
            
    

if __name__ == "__main__":
    BFS_runner("Vienna")
    routes = analyse_record()
    print_routes(routes)
        
