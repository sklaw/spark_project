from pymongo import MongoClient

dbtalk = MongoClient("localhost:27017")
metro_map = dbtalk.IBM_contest.metro_map
routes_similar_to_google_map = dbtalk.IBM_contest.routes_similar_to_google_map

station_destinations_by_directions = dbtalk.IBM_contest.station_destinations_by_directions

#station_destinations_by_directions.drop()

station_names = dbtalk.IBM_contest.station_names
stations = station_names.find()[0]['data']

if __name__ == "__main__":
    for station in stations:
        station_info = metro_map.find_one({"station_name":station})
        
        if station_info == None:
            continue
        
        directions = [i[0] for i in station_info['goto']]
        tmp_dict = {}
        for i in directions:
            tmp_dict[i] = []
        
        
        
        routes = routes_similar_to_google_map.find({'enter_station':station})
        for i  in routes:
            now_dest = i['exit_station']
            if len(i['route_by_line'][0]['subroute']) > 1:
                tmp_dict[i['route_by_line'][0]['subroute'][1][0]].append(now_dest)
        print '-'*20
        print station
        
        tmp_dict_to_save = []
        
        for key, val in tmp_dict.iteritems():
            print ' '
            print key, len(val)
            print val
            tmp_dict_to_save.append([key, val])
        
        
        
        #station_destinations_by_directions.insert({'station_name':station, 'destinations_by_directions':tmp_dict_to_save})
