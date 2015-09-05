stations = ['White Flint', 'Capitol South', 'Morgan Blvd.', 'Arlington Cemetery', 'Waterfront', 'Vienna', 'Franconia-Springfield', 'Crystal City', "Prince George's Plaza", 'Forest Glen', 'Branch Avenue', 'Grosvenor', 'West Hyattsville', 'Minnesota Avenue', 'Dunn Loring', 'McPherson Square', 'Glenmont', 'Metro Center', 'Shaw-Howard University', 'Benning Road', 'Reagan Washington National Airport', 'Takoma', 'New Carrollton', 'Tenleytown-AU', 'Foggy Bottom', 'Huntington', 'Cleveland Park', 'U Street-Cardozo', 'Naylor Road', 'Brookland', 'Mt. Vernon Square-UDC', 'Farragut North', 'Georgia Avenue-Petworth', 'Gallery Place-Chinatown', 'Stadium-Armory', 'Wiehle', 'Potomac Avenue', 'Braddock Road', 'East Falls Church', 'Wheaton', 'Spring Hill', 'Archives-Navy Memorial', 'Court House', 'Virginia Square-GMU', 'King Street', 'Congress Heights', 'Pentagon City', 'Landover', 'Cheverly', 'Woodley Park-Zoo', 'Farragut West', 'Bethesda', 'Columbia Heights', 'Greenbelt', 'Van Ness-UDC', 'Addison Road', 'Silver Spring', 'Navy Yard', 'Largo Town Center', 'Dupont Circle', 'McLean', 'Tysons Corner', 'Friendship Heights', 'Twinbrook', 'Smithsonian', 'Medical Center', 'Ballston', 'Suitland', 'Rhode Island Avenue', 'Van Dorn Street', 'Federal Center SW', 'Eastern Market', "L'Enfant Plaza", 'Deanwood', 'Rockville', 'College Park-U of MD', 'Capitol Heights', 'Greensboro', 'Union Station', 'Shady Grove', 'Judiciary Square', 'Federal Triangle', 'New York Ave', 'Clarendon', 'Anacostia', 'Southern Avenue', 'West Falls Church', 'Pentagon', 'Rosslyn', 'Fort Totten', 'Eisenhower Avenue']

metro = {'Rosslyn': {'line': ['orange', 'blue'], 'goto': [['Court House', 3, ['orange']], ['Foggy Bottom', 3, ['orange', 'blue']], ['Arlington Cemetery', 2, ['blue']]]}, 'Silver Spring': {'line': ['red'], 'goto': [['Takoma', 5, ['red']], ['Forest Glen', 4, ['red']]]}, 'Farragut North': {'line': ['red'], 'goto': [['Dupont Circle', 2, ['red']], ['Metro Center', 2, ['red']]]}, 'Southern Avenue': {'line': ['green'], 'goto': [['Congress Heights', 2, ['green']], ['Naylor Road', 3, ['green']]]}, 'Smithsonian': {'line': ['orange', 'blue'], 'goto': [["L'Enfant Plaza", 2, ['orange', 'blue']], ['Federal Triangle', 2, ['orange', 'blue']]]}, 'Dunn Loring': {'line': ['orange'], 'goto': [['Vienna', 3, ['orange']], ['West Falls Church', 4, ['orange']]]}, 'Congress Heights': {'line': ['green'], 'goto': [['Anacostia', 3, ['green']], ['Southern Avenue', 2, ['green']]]}, 'Waterfront': {'line': ['green'], 'goto': [["L'Enfant Plaza", 2, ['green']], ['Navy Yard', 2, ['green']]]}, 'Grosvenor': {'line': ['red'], 'goto': [['White Flint', 3, ['red']], ['Medical Center', 3, ['red']]]}, 'Virginia Square-GMU': {'line': ['orange'], 'goto': [['Ballston', 2, ['orange']], ['Clarendon', 1, ['orange']]]}, 'Braddock Road': {'line': ['blue', 'yellow'], 'goto': [['King Street', 2, ['blue', 'yellow']], ['Reagan Washington National Airport', 5, ['blue', 'yellow']]]}, 'East Falls Church': {'line': ['orange'], 'goto': [['West Falls Church', 4, ['orange']], ['Ballston', 4, ['orange']]]}, 'Capitol South': {'line': ['orange', 'blue'], 'goto': [['Eastern Market', 2, ['orange', 'blue']], ['Federal Center SW', 2, ['orange', 'blue']]]}, 'Naylor Road': {'line': ['green'], 'goto': [['Southern Avenue', 3, ['green']], ['Suitland', 2, ['green']]]}, 'Van Dorn Street': {'line': ['blue'], 'goto': [['King Street', 5, ['blue']], ['Franconia-Springfield', 7, ['blue']]]}, 'Union Station': {'line': ['red'], 'goto': [['Judiciary Square', 2, ['red']], ['New York Ave', 6, ['red']]]}, 'Court House': {'line': ['orange'], 'goto': [['Clarendon', 2, ['orange']], ['Rosslyn', 3, ['orange']]]}, 'Arlington Cemetery': {'line': ['blue'], 'goto': [['Rosslyn', 2, ['blue']], ['Pentagon', 3, ['blue']]]}, 'Pentagon': {'line': ['blue', 'yellow'], 'goto': [['Arlington Cemetery', 3, ['blue']], ['Pentagon City', 1, ['blue', 'yellow']], ["L'Enfant Plaza", 5, ['yellow']]]}, 'Columbia Heights': {'line': ['green'], 'goto': [['Georgia Avenue-Petworth', 3, ['green']], ['U Street-Cardozo', 2, ['green']]]}, 'Reagan Washington National Airport': {'line': ['blue', 'yellow'], 'goto': [['Braddock Road', 5, ['blue', 'yellow']], ['Crystal City', 2, ['blue', 'yellow']]]}, 'Forest Glen': {'line': ['red'], 'goto': [['Silver Spring', 4, ['red']], ['Wheaton', 3, ['red']]]}, 'Anacostia': {'line': ['green'], 'goto': [['Navy Yard', 3, ['green']], ['Congress Heights', 3, ['green']]]}, 'Largo Town Center': {'line': ['blue'], 'goto': [['Morgan Blvd.', 3, ['blue']]]}, 'Vienna': {'line': ['orange'], 'goto': [['Dunn Loring', 3, ['orange']]]}, 'Glenmont': {'line': ['red'], 'goto': [['Wheaton', 3, ['red']]]}, 'Archives-Navy Memorial': {'line': ['yellow', 'green'], 'goto': [['Gallery Place-Chinatown', 1, ['yellow', 'green']], ["L'Enfant Plaza", 2, ['yellow', 'green']]]}, 'Potomac Avenue': {'line': ['orange', 'blue'], 'goto': [['Stadium-Armory', 1, ['orange', 'blue']], ['Eastern Market', 2, ['orange', 'blue']]]}, 'College Park-U of MD': {'line': ['green'], 'goto': [['Greenbelt', 3, ['green']], ["Prince George's Plaza", 3, ['green']]]}, "L'Enfant Plaza": {'line': ['orange', 'blue', 'yellow', 'green'], 'goto': [['Federal Center SW', 2, ['orange', 'blue']], ['Smithsonian', 2, ['orange', 'blue']], ['Pentagon', 5, ['yellow']], ['Archives-Navy Memorial', 2, ['yellow', 'green']], ['Waterfront', 2, ['green']]]}, 'Clarendon': {'line': ['orange'], 'goto': [['Virginia Square-GMU', 1, ['orange']], ['Court House', 2, ['orange']]]}, 'Greenbelt': {'line': ['green'], 'goto': [['College Park-U of MD', 3, ['green']]]}, 'West Falls Church': {'line': ['orange'], 'goto': [['Dunn Loring', 4, ['orange']], ['East Falls Church', 4, ['orange']]]}, 'Branch Avenue': {'line': ['green'], 'goto': [['Suitland', 3, ['green']]]}, 'Brookland': {'line': ['red'], 'goto': [['Rhode Island Avenue', 2, ['red']], ['Fort Totten', 3, ['red']]]}, 'Foggy Bottom': {'line': ['orange', 'blue'], 'goto': [['Farragut West', 2, ['orange', 'blue']], ['Rosslyn', 3, ['orange', 'blue']]]}, 'Minnesota Avenue': {'line': ['orange'], 'goto': [['Stadium-Armory', 4, ['orange']], ['Deanwood', 2, ['orange']]]}, 'Federal Triangle': {'line': ['orange', 'blue'], 'goto': [['Smithsonian', 2, ['orange', 'blue']], ['Metro Center', 1, ['orange', 'blue']]]}, 'Federal Center SW': {'line': ['orange', 'blue'], 'goto': [['Capitol South', 2, ['orange', 'blue']], ["L'Enfant Plaza", 2, ['orange', 'blue']]]}, 'New Carrollton': {'line': ['orange'], 'goto': [['Landover', 6, ['orange']]]}, 'Huntington': {'line': ['yellow'], 'goto': [['Eisenhower Avenue', 3, ['yellow']]]}, 'Woodley Park-Zoo': {'line': ['red'], 'goto': [['Cleveland Park', 2, ['red']], ['Dupont Circle', 2, ['red']]]}, 'Crystal City': {'line': ['blue', 'yellow'], 'goto': [['Reagan Washington National Airport', 2, ['blue', 'yellow']], ['Pentagon City', 2, ['blue', 'yellow']]]}, 'White Flint': {'line': ['red'], 'goto': [['Twinbrook', 3, ['red']], ['Grosvenor', 3, ['red']]]}, 'Gallery Place-Chinatown': {'line': ['yellow', 'green', 'red'], 'goto': [['Mt. Vernon Square-UDC', 2, ['yellow', 'green']], ['Archives-Navy Memorial', 1, ['yellow', 'green']], ['Metro Center', 2, ['red']], ['Judiciary Square', 4, ['red']]]}, 'Addison Road': {'line': ['blue'], 'goto': [['Morgan Blvd.', 3, ['blue']], ['Capitol Heights', 3, ['blue']]]}, 'Cleveland Park': {'line': ['red'], 'goto': [['Van Ness-UDC', 2, ['red']], ['Woodley Park-Zoo', 2, ['red']]]}, 'Takoma': {'line': ['red'], 'goto': [['Fort Totten', 2, ['red']], ['Silver Spring', 5, ['red']]]}, 'West Hyattsville': {'line': ['green'], 'goto': [["Prince George's Plaza", 3, ['green']], ['Fort Totten', 3, ['green']]]}, 'Bethesda': {'line': ['red'], 'goto': [['Medical Center', 3, ['red']], ['Friendship Heights', 2, ['red']]]}, 'Shady Grove': {'line': ['red'], 'goto': [['Rockville', 3, ['red']]]}, "Prince George's Plaza": {'line': ['green'], 'goto': [['College Park-U of MD', 3, ['green']], ['West Hyattsville', 3, ['green']]]}, 'Rhode Island Avenue': {'line': ['red'], 'goto': [['New York Ave', 5, ['red']], ['Brookland', 2, ['red']]]}, 'Judiciary Square': {'line': ['red'], 'goto': [['Gallery Place-Chinatown', 4, ['red']], ['Union Station', 2, ['red']]]}, 'Georgia Avenue-Petworth': {'line': ['green'], 'goto': [['Fort Totten', 3, ['green']], ['Columbia Heights', 3, ['green']]]}, 'Stadium-Armory': {'line': ['orange', 'blue'], 'goto': [['Minnesota Avenue', 4, ['orange']], ['Benning Road', 3, ['blue']], ['Potomac Avenue', 1, ['orange', 'blue']]]}, 'Eastern Market': {'line': ['orange', 'blue'], 'goto': [['Potomac Avenue', 2, ['orange', 'blue']], ['Capitol South', 2, ['orange', 'blue']]]}, 'Van Ness-UDC': {'line': ['red'], 'goto': [['Tenleytown-AU', 2, ['red']], ['Cleveland Park', 2, ['red']]]}, 'Capitol Heights': {'line': ['blue'], 'goto': [['Addison Road', 3, ['blue']], ['Benning Road', 3, ['blue']]]}, 'Rockville': {'line': ['red'], 'goto': [['Shady Grove', 3, ['red']], ['Twinbrook', 3, ['red']]]}, 'New York Ave': {'line': ['red'], 'goto': [['Union Station', 6, ['red']], ['Rhode Island Avenue', 5, ['red']]]}, 'Fort Totten': {'line': ['green', 'red'], 'goto': [['West Hyattsville', 3, ['green']], ['Georgia Avenue-Petworth', 3, ['green']], ['Brookland', 3, ['red']], ['Takoma', 2, ['red']]]}, 'Shaw-Howard University': {'line': ['green'], 'goto': [['U Street-Cardozo', 2, ['green']], ['Mt. Vernon Square-UDC', 1, ['green']]]}, 'Friendship Heights': {'line': ['red'], 'goto': [['Bethesda', 2, ['red']], ['Tenleytown-AU', 2, ['red']]]}, 'Suitland': {'line': ['green'], 'goto': [['Naylor Road', 2, ['green']], ['Branch Avenue', 3, ['green']]]}, 'Ballston': {'line': ['orange'], 'goto': [['East Falls Church', 4, ['orange']], ['Virginia Square-GMU', 2, ['orange']]]}, 'Eisenhower Avenue': {'line': ['yellow'], 'goto': [['Huntington', 3, ['yellow']], ['King Street', 2, ['yellow']]]}, 'Pentagon City': {'line': ['blue', 'yellow'], 'goto': [['Crystal City', 2, ['blue', 'yellow']], ['Pentagon', 1, ['blue', 'yellow']]]}, 'Metro Center': {'line': ['orange', 'blue', 'red'], 'goto': [['Federal Triangle', 1, ['orange', 'blue']], ['McPherson Square', 4, ['orange', 'blue']], ['Farragut North', 2, ['red']], ['Gallery Place-Chinatown', 2, ['red']]]}, 'Wheaton': {'line': ['red'], 'goto': [['Forest Glen', 3, ['red']], ['Glenmont', 3, ['red']]]}, 'Tenleytown-AU': {'line': ['red'], 'goto': [['Friendship Heights', 2, ['red']], ['Van Ness-UDC', 2, ['red']]]}, 'Deanwood': {'line': ['orange'], 'goto': [['Minnesota Avenue', 2, ['orange']], ['Cheverly', 2, ['orange']]]}, 'Medical Center': {'line': ['red'], 'goto': [['Grosvenor', 3, ['red']], ['Bethesda', 3, ['red']]]}, 'Navy Yard': {'line': ['green'], 'goto': [['Waterfront', 2, ['green']], ['Anacostia', 3, ['green']]]}, 'Morgan Blvd.': {'line': ['blue'], 'goto': [['Largo Town Center', 3, ['blue']], ['Addison Road', 3, ['blue']]]}, 'Mt. Vernon Square-UDC': {'line': ['yellow', 'green'], 'goto': [['Shaw-Howard University', 1, ['green']], ['Gallery Place-Chinatown', 2, ['yellow', 'green']]]}, 'Dupont Circle': {'line': ['red'], 'goto': [['Woodley Park-Zoo', 2, ['red']], ['Farragut North', 2, ['red']]]}, 'McPherson Square': {'line': ['orange', 'blue'], 'goto': [['Metro Center', 4, ['orange', 'blue']], ['Farragut West', 2, ['orange', 'blue']]]}, 'Landover': {'line': ['orange'], 'goto': [['Cheverly', 7, ['orange']], ['New Carrollton', 6, ['orange']]]}, 'Benning Road': {'line': ['blue'], 'goto': [['Capitol Heights', 3, ['blue']], ['Stadium-Armory', 3, ['blue']]]}, 'Farragut West': {'line': ['orange', 'blue'], 'goto': [['McPherson Square', 2, ['orange', 'blue']], ['Foggy Bottom', 2, ['orange', 'blue']]]}, 'Twinbrook': {'line': ['red'], 'goto': [['Rockville', 3, ['red']], ['White Flint', 3, ['red']]]}, 'Cheverly': {'line': ['orange'], 'goto': [['Deanwood', 2, ['orange']], ['Landover', 7, ['orange']]]}, 'U Street-Cardozo': {'line': ['green'], 'goto': [['Columbia Heights', 2, ['green']], ['Shaw-Howard University', 2, ['green']]]}, 'King Street': {'line': ['blue', 'yellow'], 'goto': [['Van Dorn Street', 5, ['blue']], ['Eisenhower Avenue', 2, ['yellow']], ['Braddock Road', 2, ['blue', 'yellow']]]}, 'Franconia-Springfield': {'line': ['blue'], 'goto': [['Van Dorn Street', 7, ['blue']]]}}













def check_valid():
    for key in metro:
        if (key not in stations):
            print key+" invalid."
        for i in metro[key]["goto"]:
            if i[0] not in stations:
                print i[0]+" invalid in",[key]

check_valid()

print "go!"

line = "red"

now_station = "Shady Grove"

while(1):
    item = metro[now_station]
    candidate = []
    for c in item['goto']:
        #print c
        if len(c) == 2 and line in metro[c[0]]['line']:
            candidate.append(c[0])
        elif len(c) == 3 and line in metro[c[0]]['line'] and line not in c[2]:
            print 'len(c) is 3 and we need to append this line!',c
            candidate.append(c[0])
    
    index = 0
    
    if len(candidate) > 1:
        print now_station,"I'm confused! which of the following belongs to line[", line,"]? Answer with index"
        print candidate
        index = int(raw_input())
    elif len(candidate) == 0:
        print '-'*20
        print now_station, now_station_goto
        print '*'*20
        print metro
        print '*'*20
        break
    
    next_station = candidate[index]
    
    now_station_goto = metro[now_station]['goto']
    next_station_goto = metro[next_station]['goto']
    
    now_station_tup = [i for i in now_station_goto if i[0] == next_station][0]
    next_station_tup = [i for i in next_station_goto if i[0] == now_station][0]
    
    now_station_goto = [i for i in now_station_goto if i != now_station_tup]
    next_station_goto = [i for i in next_station_goto if i != next_station_tup]
    
    if len(now_station_tup) != 3:
        now_station_tup = [now_station_tup[0], now_station_tup[1], []]
    
    if len(next_station_tup) != 3:
        next_station_tup = [next_station_tup[0], next_station_tup[1], []]

    now_station_tup[2].append(line)
    next_station_tup[2].append(line)

    now_station_goto.append([now_station_tup[0], now_station_tup[1], now_station_tup[2]])        
    next_station_goto.append([next_station_tup[0], next_station_tup[1], next_station_tup[2]])
    
    #print '*'*20
    #print metro
    #print '*'*20
    
    
    print '-'*20
    print now_station, now_station_goto
    #print "next_station_goto", next_station_goto
    
    
    metro[now_station]['goto'] = now_station_goto
    metro[next_station]['goto'] = next_station_goto
    
    
    now_station = next_station

    
    
    
    