from pymongo import MongoClient

dbtalk = MongoClient("localhost:27017")
metro_map = dbtalk.IBM_contest.metro_map
station_names = dbtalk.IBM_contest.station_names
stations = station_names.find()[0]['data']





record = {}

get_possible_interchange_time = None

#def try_to_mark(previouse_to_current_line, current_to_next_line, next_node_cost_without_interchange, current_node, next_node):
def try_to_mark(current_platform_arrived, current_platform_to_depart, origin_to_next_ideal_cost, current_node, next_node, current_nodes):
    global record
    
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
            if next_node not in current_nodes:
                current_nodes.append(next_node)
    
    

def BFS(current_nodes):
    global record
    if len(current_nodes) == 0:
        return
    
    pick = None
    
    for idx, val in enumerate(current_nodes):
        if pick == None:
            pick = idx
        else:
            candidate_platform = None
            for i in record[val]['info_by_platforms'].keys():
                if candidate_platform == None:
                    candidate_platform = i
                elif record[val]['info_by_platforms'][i]['cost'] < record[val]['info_by_platforms'][candidate_platform]['cost']:
                    candidate_platform = i
            
            if record[val]['info_by_platforms'][candidate_platform]['cost'] < current_nodes[pick]:
                pick = idx
    
    current_node = current_nodes[idx]
    current_nodes.pop(idx)
    
    if True:
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
                    try_to_mark(None, current_platform_to_depart, origin_to_next_ideal_cost, current_node, next_node, current_nodes)
                else:
                    for current_platform_arrived in record[current_node]['info_by_platforms'].keys():
                        origin_to_next_ideal_cost = record[current_node]['info_by_platforms'][current_platform_to_depart]['cost']+current_to_next_ideal_cost
                        try_to_mark(current_platform_arrived, current_platform_to_depart, origin_to_next_ideal_cost, current_node, next_node, current_nodes)

    BFS(current_nodes)




def BFS_runner(origin_node, func):
    global record, get_possible_interchange_time
    get_possible_interchange_time = func
    record = {}
    origin_node_info = metro_map.find_one({"station_name":origin_node})
    
    if origin_node_info == None:
        return {}
    
    origin_departing_platfroms = origin_node_info['line']

    record[origin_node] = {'info_by_platforms':{}}
    for origin_per_platform in origin_departing_platfroms:
        record[origin_node]['info_by_platforms'][origin_per_platform] = {}
        record[origin_node]['info_by_platforms'][origin_per_platform]['cost'] = 0
        record[origin_node]['info_by_platforms'][origin_per_platform]['previous_station'] = ""
        record[origin_node]['info_by_platforms'][origin_per_platform]['depart_platform_of_previous_station'] = ""
    
    BFS([origin_node])
    return record