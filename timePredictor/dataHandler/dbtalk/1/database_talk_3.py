from pymongo import MongoClient

dbtalk = MongoClient("localhost:27017")
metro_map = dbtalk.IBM_contest.metro_map

stations = ['White Flint', 'Capitol South', 'Morgan Blvd.', 'Arlington Cemetery', 'Waterfront', 'Vienna', 'Franconia-Springfield', 'Crystal City', "Prince George's Plaza", 'Forest Glen', 'Branch Avenue', 'Grosvenor', 'West Hyattsville', 'Minnesota Avenue', 'Dunn Loring', 'McPherson Square', 'Glenmont', 'Metro Center', 'Shaw-Howard University', 'Benning Road', 'Reagan Washington National Airport', 'Takoma', 'New Carrollton', 'Tenleytown-AU', 'Foggy Bottom', 'Huntington', 'Cleveland Park', 'U Street-Cardozo', 'Naylor Road', 'Brookland', 'Mt. Vernon Square-UDC', 'Farragut North', 'Georgia Avenue-Petworth', 'Gallery Place-Chinatown', 'Stadium-Armory', 'Wiehle', 'Potomac Avenue', 'Braddock Road', 'East Falls Church', 'Wheaton', 'Spring Hill', 'Archives-Navy Memorial', 'Court House', 'Virginia Square-GMU', 'King Street', 'Congress Heights', 'Pentagon City', 'Landover', 'Cheverly', 'Woodley Park-Zoo', 'Farragut West', 'Bethesda', 'Columbia Heights', 'Greenbelt', 'Van Ness-UDC', 'Addison Road', 'Silver Spring', 'Navy Yard', 'Largo Town Center', 'Dupont Circle', 'McLean', 'Tysons Corner', 'Friendship Heights', 'Twinbrook', 'Smithsonian', 'Medical Center', 'Ballston', 'Suitland', 'Rhode Island Avenue', 'Van Dorn Street', 'Federal Center SW', 'Eastern Market', "L'Enfant Plaza", 'Deanwood', 'Rockville', 'College Park-U of MD', 'Capitol Heights', 'Greensboro', 'Union Station', 'Shady Grove', 'Judiciary Square', 'Federal Triangle', 'New York Ave', 'Clarendon', 'Anacostia', 'Southern Avenue', 'West Falls Church', 'Pentagon', 'Rosslyn', 'Fort Totten', 'Eisenhower Avenue']

interchange_time = 10

def get_route(station_name, result):
    for k, v in result.iteritems():
        print '1'*20
        print station_name, "->", k

        for line in v["least_cost_by_line"].keys():
            if line == "":
                continue
                
            print '2'*20
            print 'end with', line, v['least_cost_by_line'][line]['cost']
  
            now_station = k
            now_line = line
             
            route = []
             
            while True:
            
                if now_station == "":
                    break;
                route.append((now_station, result[now_station]['least_cost_by_line'][now_line]['cost']))
                
                if now_line == "":
                    break;
                route.append(now_line)
                
                pre_station = result[now_station]['least_cost_by_line'][now_line]['pre_station']
                pre_line = result[now_station]['least_cost_by_line'][now_line]['pre_line']
                
                now_station = pre_station
                now_line = pre_line
                

            route = route[::-1]
            
            
            if len(route) < 3:
                continue
            print '3'*20
            print route
            print " " 
              
            to_print = route[1]+":"
            
            for i in range(len(route)):
                if i%2 == 0:
                    to_print += str(route[i])+"->"
                elif i>2 and route[i-2] != route[i]:
                    print to_print
                    to_print = route[i]+":"
            print to_print
                
            
#current_station < line < pre_station < pre_line
            
def BFS(station_name):

    result = {
                station_name:{
                              "least_cost_by_line":{
                                                    "": {"cost":0,
                                                            "pre_station":"",
                                                            "pre_line":""
                                                           }
                                                     }  

                             }
             }

    
    current_points = [station_name]
 
    while len(current_points) != 0:
        future_current_points = []
        
        #raw_input()
        print current_points
        for current_station in current_points:
            
            print "visit", current_station
            
            current_station_info = metro_map.find_one({"station_name":current_station})       
            for i in current_station_info["goto"]:
                next_station_name = i[0]
                next_station_ideal_cost = i[1]
                next_station_info = metro_map.find_one({"station_name":next_station_name})
                for line_to_next_station in set(current_station_info['line']).intersection(next_station_info['line']):
                    for line_to_current_station in result[current_station]['least_cost_by_line'].keys():
                        
                        
                        next_station_cost = result[current_station]['least_cost_by_line'][line_to_current_station]['cost']+next_station_ideal_cost
                        
                        if line_to_current_station == "" or line_to_current_station == line_to_next_station:
                            pass
                        else:
                            next_station_cost += interchange_time
                        
                        #try to mark the next station
                        mark = False
                        
                        if next_station_name not in result.keys():
                            mark = True
                            result[next_station_name] = {}
                            result[next_station_name]['least_cost_by_line'] = {}
                            result[next_station_name]['least_cost_by_line'][line_to_next_station] = {}
                            
                        elif line_to_next_station not in result[next_station_name]['least_cost_by_line'].keys():
                            mark = True
                            result[next_station_name]['least_cost_by_line'][line_to_next_station] = {}
                        
                        elif next_station_cost < result[next_station_name]['least_cost_by_line'][line_to_next_station]['cost']:
                            mark = True
                        else:
                            mark = False
                            
                        if mark:
                            result[next_station_name]['least_cost_by_line'][line_to_next_station]['cost'] = next_station_cost
                            result[next_station_name]['least_cost_by_line'][line_to_next_station]['pre_station'] = current_station
                            result[next_station_name]['least_cost_by_line'][line_to_next_station]['pre_line'] = line_to_current_station
                            
                            if next_station_name not in future_current_points:
                                future_current_points.append(next_station_name)
                    
        current_points = future_current_points    
    get_route(station_name, result)




database_directions = dbtalk.IBM_contest.directions




    
    
if __name__ == "__main__":
    BFS("Vienna")
    
        
