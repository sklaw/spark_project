from pymongo import MongoClient

dbtalk = MongoClient("localhost:27017")
metro_map = dbtalk.IBM_contest.metro_map

stations = ['White Flint', 'Capitol South', 'Morgan Blvd.', 'Arlington Cemetery', 'Waterfront', 'Vienna', 'Franconia-Springfield', 'Crystal City', "Prince George's Plaza", 'Forest Glen', 'Branch Avenue', 'Grosvenor', 'West Hyattsville', 'Minnesota Avenue', 'Dunn Loring', 'McPherson Square', 'Glenmont', 'Metro Center', 'Shaw-Howard University', 'Benning Road', 'Reagan Washington National Airport', 'Takoma', 'New Carrollton', 'Tenleytown-AU', 'Foggy Bottom', 'Huntington', 'Cleveland Park', 'U Street-Cardozo', 'Naylor Road', 'Brookland', 'Mt. Vernon Square-UDC', 'Farragut North', 'Georgia Avenue-Petworth', 'Gallery Place-Chinatown', 'Stadium-Armory', 'Wiehle', 'Potomac Avenue', 'Braddock Road', 'East Falls Church', 'Wheaton', 'Spring Hill', 'Archives-Navy Memorial', 'Court House', 'Virginia Square-GMU', 'King Street', 'Congress Heights', 'Pentagon City', 'Landover', 'Cheverly', 'Woodley Park-Zoo', 'Farragut West', 'Bethesda', 'Columbia Heights', 'Greenbelt', 'Van Ness-UDC', 'Addison Road', 'Silver Spring', 'Navy Yard', 'Largo Town Center', 'Dupont Circle', 'McLean', 'Tysons Corner', 'Friendship Heights', 'Twinbrook', 'Smithsonian', 'Medical Center', 'Ballston', 'Suitland', 'Rhode Island Avenue', 'Van Dorn Street', 'Federal Center SW', 'Eastern Market', "L'Enfant Plaza", 'Deanwood', 'Rockville', 'College Park-U of MD', 'Capitol Heights', 'Greensboro', 'Union Station', 'Shady Grove', 'Judiciary Square', 'Federal Triangle', 'New York Ave', 'Clarendon', 'Anacostia', 'Southern Avenue', 'West Falls Church', 'Pentagon', 'Rosslyn', 'Fort Totten', 'Eisenhower Avenue']

def BFS(visited, visiting):
    while len(visiting) != 0:
        will_visit = {}
        for key in visiting.keys():
            visited[key] = {}
            
            if 'cost' in visiting[key].keys():
                visited[key]['cost'] = visiting[key]['cost']
            else:
                visited[key]['cost'] = 0
            
            
            
            visited[key]['cost'] = visiting[key]['cost']
            visited[key]['on_line'] = visiting[key]['on_line']
            

            
            if 'pre_station' in visiting[key].keys():
                visited[key]["pre_station"] = visiting[key]['pre_station']
            
            station_info = metro_map.find_one({"station_name":key})
            

            
            for i in station_info["goto"]:

                for l in visiting[key]['on_line']:
                    cost = visited[key]['cost']+i[1]
                    if l not in station_info['line']:
                        cost += 5
                        
                    if i[0] not in visited.keys():
                        will_visit[i[0]] = {}
                        will_visit[i[0]]['cost'] = cost
                        will_visit[i[0]]['on_line'] = l
                        will_visit[i[0]]['pre_station'] = key
                        
                    elif visited[i[0]]['cost'] > cost:
                        will_visit[i[0]] = {}
                        will_visit[i[0]]['cost'] = cost
                        will_visit[i[0]]['on_line'] = l
                        will_visit[i[0]]['pre_station'] = key
                    
        visiting = will_visit
        
    return visited



database_directions = dbtalk.IBM_contest.directions

database_directions.drop()

def get_directions(station_name):

    directions = {}


    station_info = metro_map.find_one({"station_name":station_name})

    if station_info == None:
        return

    goto = station_info["goto"]

    for d in goto:
        visited = {}
        visited[station_name] = {}
        visited[station_name]['cost'] = 0

        visiting = {}
        visiting[d[0]] = {}
        visiting[d[0]]['cost'] = 0+d[1]
        visiting[d[0]]['pre_station'] = station_name
        
        station_info_2 = metro_map.find_one({"station_name":d[0]})
        
        visiting[d[0]]['on_line'] = set(station_info['line']).intersection(station_info_2['line'])
        directions[d[0]] = BFS(visited, visiting)

    for i in stations:
        candidate = []
        for j in directions.keys():
            if i in directions[j].keys():
                candidate.append(j)
            
        to_pick = None
            
        for j in candidate:
            if to_pick == None:
                to_pick = j
            elif directions[to_pick][i] > directions[j][i]:
                directions[to_pick].pop(i)
                to_pick = j
            else:
                directions[j].pop(i)


    #for i in directions.keys():
    #    print i+' '+str(len(directions[i]))+' '+'*'*20
    #    for j in sorted(directions[i].items(), key=lambda x: x[1]):
    #        print j

    
    
    
    
    
    
    directions_to_save = []
    
    for i in directions.keys():
        directions_to_save.append([i, [j[0] for j in directions[i].items()]])
    
    print "*"*30
    print station_name
    for i in directions_to_save:
        print "-"*15
        print i[0]+" "+str(len(i[1]))
        print i[1]
    
    database_directions.insert({"station_name":station_name, "directions":directions_to_save})
    
    
if __name__ == "__main__":
    pass
    for i in stations:
        get_directions(i)
        
