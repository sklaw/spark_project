stations = ['White Flint', 'Capitol South', 'Morgan Blvd.', 'Arlington Cemetery', 'Waterfront', 'Vienna', 'Franconia-Springfield', 'Crystal City', "Prince George's Plaza", 'Forest Glen', 'Branch Avenue', 'Grosvenor', 'West Hyattsville', 'Minnesota Avenue', 'Dunn Loring', 'McPherson Square', 'Glenmont', 'Metro Center', 'Shaw-Howard University', 'Benning Road', 'Reagan Washington National Airport', 'Takoma', 'New Carrollton', 'Tenleytown-AU', 'Foggy Bottom', 'Huntington', 'Cleveland Park', 'U Street-Cardozo', 'Naylor Road', 'Brookland', 'Mt. Vernon Square-UDC', 'Farragut North', 'Georgia Avenue-Petworth', 'Gallery Place-Chinatown', 'Stadium-Armory', 'Wiehle', 'Potomac Avenue', 'Braddock Road', 'East Falls Church', 'Wheaton', 'Spring Hill', 'Archives-Navy Memorial', 'Court House', 'Virginia Square-GMU', 'King Street', 'Congress Heights', 'Pentagon City', 'Landover', 'Cheverly', 'Woodley Park-Zoo', 'Farragut West', 'Bethesda', 'Columbia Heights', 'Greenbelt', 'Van Ness-UDC', 'Addison Road', 'Silver Spring', 'Navy Yard', 'Largo Town Center', 'Dupont Circle', 'McLean', 'Tysons Corner', 'Friendship Heights', 'Twinbrook', 'Smithsonian', 'Medical Center', 'Ballston', 'Suitland', 'Rhode Island Avenue', 'Van Dorn Street', 'Federal Center SW', 'Eastern Market', "L'Enfant Plaza", 'Deanwood', 'Rockville', 'College Park-U of MD', 'Capitol Heights', 'Greensboro', 'Union Station', 'Shady Grove', 'Judiciary Square', 'Federal Triangle', 'New York Ave', 'Clarendon', 'Anacostia', 'Southern Avenue', 'West Falls Church', 'Pentagon', 'Rosslyn', 'Fort Totten', 'Eisenhower Avenue']

metro = {'Rosslyn': {'line': ['orange', 'blue'], 'goto': [('Court House', 3), ('Foggy Bottom', 3), ('Arlington Cemetery', 2)]}, 'Woodley Park-Zoo': {'line': ['red'], 'goto': [('Cleveland Park', 2), ('Dupont Circle', 2)]}, 'Crystal City': {'line': ['blue', 'yellow'], 'goto': [('Reagan Washington National Airport', 2), ('Pentagon City', 2)]}, 'White Flint': {'line': ['red'], 'goto': [('Twinbrook', 3), ('Grosvenor', 3)]}, 'Gallery Place-Chinatown': {'line': ['yellow', 'green', 'red'], 'goto': [('Archives-Navy Memorial', 1), ('Mt. Vernon Square-UDC', 2), ('Metro Center', 2), ('Judiciary Square', 4)]}, 'Addison Road': {'line': ['blue'], 'goto': [('Capitol Heights', 3), ('Morgan Blvd.', 3)]}, 'Cleveland Park': {'line': ['red'], 'goto': [('Van Ness-UDC', 2), ('Woodley Park-Zoo', 2)]}, 'Takoma': {'line': ['red'], 'goto': [('Fort Totten', 2), ('Silver Spring', 5)]}, 'West Hyattsville': {'line': ['green'], 'goto': [('Fort Totten', 3), ("Prince George's Plaza", 3)]}, 'Bethesda': {'line': ['red'], 'goto': [('Medical Center', 3), ('Friendship Heights', 2)]}, 'Shady Grove': {'line': ['red'], 'goto': [('Rockville', 3)]}, "Prince George's Plaza": {'line': ['green'], 'goto': [('West Hyattsville', 3), ('College Park-U of MD', 3)]}, 'Rhode Island Avenue': {'line': ['red'], 'goto': [('New York Ave', 5), ('Brookland', 2)]}, 'Southern Avenue': {'line': ['green'], 'goto': [('Naylor Road', 3), ('Congress Heights', 2)]}, 'Georgia Avenue-Petworth': {'line': ['green'], 'goto': [('Columbia Heights', 3), ('Fort Totten', 3)]}, 'Stadium-Armory': {'line': ['orange', 'blue'], 'goto': [('Potomac Avenue', 1), ('Minnesota Avenue', 4), ('Benning Road', 3)]}, 'Smithsonian': {'line': ['orange', 'blue'], 'goto': [('Federal Triangle', 2), ("L'Enfant Plaza", 2)]}, 'Dunn Loring': {'line': ['orange'], 'goto': [('Vienna', 3), ('West Falls Church', 4)]}, 'Eastern Market': {'line': ['orange', 'blue'], 'goto': [('Capitol South', 2), ('Potomac Avenue', 2)]}, 'Waterfront': {'line': ['green'], 'goto': [('Navy Yard', 2), ("L'Enfant Plaza", 2)]}, 'Van Ness-UDC': {'line': ['red'], 'goto': [('Tenleytown-AU', 2), ('Cleveland Park', 2)]}, 'Capitol Heights': {'line': ['blue'], 'goto': [('Benning Road', 3), ('Addison Road', 3)]}, 'Union Station': {'line': ['red'], 'goto': [('Judiciary Square', 2), ('New York Ave', 6)]}, 'Virginia Square-GMU': {'line': ['orange'], 'goto': [('Ballston', 2), ('Clarendon', 1)]}, 'Braddock Road': {'line': ['blue', 'yellow'], 'goto': [('King Street', 2), ('Reagan Washington National Airport', 5)]}, 'Rockville': {'line': ['red'], 'goto': [('Shady Grove', 3), ('Twinbrook', 3)]}, 'New York Ave': {'line': ['red'], 'goto': [('Union Station', 6), ('Rhode Island Avenue', 5)]}, 'East Falls Church': {'line': ['orange'], 'goto': [('West Falls Church', 4), ('Ballston', 4)]}, 'Capitol South': {'line': ['orange', 'blue'], 'goto': [('Federal Center SW', 2), ('Eastern Market', 2)]}, 'Naylor Road': {'line': ['green'], 'goto': [('Suitland', 2), ('Southern Avenue', 3)]}, 'Fort Totten': {'line': ['green', 'red'], 'goto': [('Georgia Avenue-Petworth', 3), ('West Hyattsville', 3), ('Brookland', 3), ('Takoma', 2)]}, 'Van Dorn Street': {'line': ['blue'], 'goto': [('Franconia-Springfield', 7), ('King Street', 5)]}, 'Shaw-Howard University': {'line': ['green'], 'goto': [('Mt. Vernon Square-UDC', 1), ('U Street-Cardozo', 2)]}, 'Columbia Heights': {'line': ['green'], 'goto': [('U Street-Cardozo', 2), ('Georgia Avenue-Petworth', 3)]}, 'Court House': {'line': ['orange'], 'goto': [('Clarendon', 2), ('Rosslyn', 3)]}, 'Suitland': {'line': ['green'], 'goto': [('Branch Avenue', 3), ('Naylor Road', 2)]}, 'Ballston': {'line': ['orange'], 'goto': [('East Falls Church', 4), ('Virginia Square-GMU', 2)]}, 'Eisenhower Avenue': {'line': ['yellow'], 'goto': [('Huntington', 3), ('King Street', 2)]}, 'Arlington Cemetery': {'line': ['blue'], 'goto': [('Pentagon', 3), ('Rosslyn', 2)]}, 'Pentagon': {'line': ['blue', 'yellow'], 'goto': [('Pentagon City', 1), ('Arlington Cemetery', 3), ("L'Enfant Plaza", 5)]}, 'Pentagon City': {'line': ['blue', 'yellow'], 'goto': [('Crystal City', 2), ('Pentagon', 1)]}, 'Metro Center': {'line': ['orange', 'blue', 'red'], 'goto': [('McPherson Square', 4), ('Federal Triangle', 1), ('Farragut North', 2), ('Gallery Place-Chinatown', 2)]}, 'Friendship Heights': {'line': ['red'], 'goto': [('Bethesda', 2), ('Tenleytown-AU', 2)]}, 'Reagan Washington National Airport': {'line': ['blue', 'yellow'], 'goto': [('Braddock Road', 5), ('Crystal City', 2)]}, 'Forest Glen': {'line': ['red'], 'goto': [('Silver Spring', 4), ('Wheaton', 3)]}, 'Anacostia': {'line': ['green'], 'goto': [('Congress Heights', 3), ('Navy Yard', 3)]}, 'Tenleytown-AU': {'line': ['red'], 'goto': [('Friendship Heights', 2), ('Van Ness-UDC', 2)]}, 'Largo Town Center': {'line': ['blue'], 'goto': [('Morgan Blvd.', 3)]}, 'Judiciary Square': {'line': ['red'], 'goto': [('Gallery Place-Chinatown', 4), ('Union Station', 2)]}, 'Vienna': {'line': ['orange'], 'goto': [('Dunn Loring', 3)]}, 'Medical Center': {'line': ['red'], 'goto': [('Grosvenor', 3), ('Bethesda', 3)]}, 'Farragut North': {'line': ['red'], 'goto': [('Dupont Circle', 2), ('Metro Center', 2)]}, 'Navy Yard': {'line': ['green'], 'goto': [('Anacostia', 3), ('Waterfront', 2)]}, 'Morgan Blvd.': {'line': ['blue'], 'goto': [('Addison Road', 3), ('Largo Town Center', 3)]}, 'Archives-Navy Memorial': {'line': ['yellow', 'green'], 'goto': [("L'Enfant Plaza", 2), ('Gallery Place-Chinatown', 1)]}, 'Potomac Avenue': {'line': ['orange', 'blue'], 'goto': [('Eastern Market', 2), ('Stadium-Armory', 1)]}, 'College Park-U of MD': {'line': ['green'], 'goto': [("Prince George's Plaza", 3), ('Greenbelt', 3)]}, "L'Enfant Plaza": {'line': ['orange', 'blue', 'yellow', 'green'], 'goto': [('Smithsonian', 2), ('Federal Center SW', 2), ('Pentagon', 5), ('Archives-Navy Memorial', 2), ('Waterfront', 2)]}, 'Clarendon': {'line': ['orange'], 'goto': [('Virginia Square-GMU', 1), ('Court House', 2)]}, 'Mt. Vernon Square-UDC': {'line': ['yellow', 'green'], 'goto': [('Gallery Place-Chinatown', 2), ('Shaw-Howard University', 1)]}, 'Greenbelt': {'line': ['green'], 'goto': [('College Park-U of MD', 3)]}, 'Dupont Circle': {'line': ['red'], 'goto': [('Woodley Park-Zoo', 2), ('Farragut North', 2)]}, 'Deanwood': {'line': ['orange'], 'goto': [('Minnesota Avenue', 2), ('Cheverly', 2)]}, 'McPherson Square': {'line': ['orange', 'blue'], 'goto': [('Farragut West', 2), ('Metro Center', 4)]}, 'Landover': {'line': ['orange'], 'goto': [('Cheverly', 7), ('New Carrollton', 6)]}, 'West Falls Church': {'line': ['orange'], 'goto': [('Dunn Loring', 4), ('East Falls Church', 4)]}, 'Branch Avenue': {'line': ['green'], 'goto': [('Suitland', 3)]}, 'Brookland': {'line': ['red'], 'goto': [('Rhode Island Avenue', 2), ('Fort Totten', 3)]}, 'Glenmont': {'line': ['red'], 'goto': [('Wheaton', 3)]}, 'Foggy Bottom': {'line': ['orange', 'blue'], 'goto': [('Rosslyn', 3), ('Farragut West', 2)]}, 'Wheaton': {'line': ['red'], 'goto': [('Forest Glen', 3), ('Glenmont', 3)]}, 'Benning Road': {'line': ['blue'], 'goto': [('Stadium-Armory', 3), ('Capitol Heights', 3)]}, 'Minnesota Avenue': {'line': ['orange'], 'goto': [('Stadium-Armory', 4), ('Deanwood', 2)]}, 'Farragut West': {'line': ['orange', 'blue'], 'goto': [('Foggy Bottom', 2), ('McPherson Square', 2)]}, 'Federal Triangle': {'line': ['orange', 'blue'], 'goto': [('Metro Center', 1), ('Smithsonian', 2)]}, 'Grosvenor': {'line': ['red'], 'goto': [('White Flint', 3), ('Medical Center', 3)]}, 'Twinbrook': {'line': ['red'], 'goto': [('Rockville', 3), ('White Flint', 3)]}, 'Silver Spring': {'line': ['red'], 'goto': [('Takoma', 5), ('Forest Glen', 4)]}, 'Federal Center SW': {'line': ['orange', 'blue'], 'goto': [("L'Enfant Plaza", 2), ('Capitol South', 2)]}, 'New Carrollton': {'line': ['orange'], 'goto': [('Landover', 6)]}, 'Cheverly': {'line': ['orange'], 'goto': [('Deanwood', 2), ('Landover', 7)]}, 'U Street-Cardozo': {'line': ['green'], 'goto': [('Shaw-Howard University', 2), ('Columbia Heights', 2)]}, 'King Street': {'line': ['blue', 'yellow'], 'goto': [('Van Dorn Street', 5), ('Braddock Road', 2), ('Eisenhower Avenue', 2)]}, 'Huntington': {'line': ['yellow'], 'goto': [('Eisenhower Avenue', 3)]}, 'Congress Heights': {'line': ['green'], 'goto': [('Southern Avenue', 2), ('Anacostia', 3)]}, 'Franconia-Springfield': {'line': ['blue'], 'goto': [('Van Dorn Street', 7)]}}








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


ex_station = "Van Ness-UDC"

while(1):
    tmp = raw_input()
    
    if tmp == 'end':
        break
        
    items = tmp.split(',')
    now_station = items[0]
    time = int(items[1])
    
    if (ex_station not in stations):
        print "ex_staion invalid."
        continue
    
    if (now_station not in stations):
        print "now_staion invalid."
        continue

    
    if ex_station not in metro.keys():
        metro[ex_station] = {"goto":[], "line":[]}
        metro[ex_station]["line"].append(line)
    
    if now_station not in metro.keys():
        metro[now_station] = {"goto":[], "line":[]}
        
    metro[now_station]["line"].append(line)
    
    if time != -1:      
        tup = (ex_station, time)
    
        if (tup not in metro[now_station]["goto"]):
            metro[now_station]["goto"].append(tup)
    
        tup = (now_station, time)
        if (tup not in metro[ex_station]["goto"]):
            metro[ex_station]["goto"].append(tup)
    
    
    
    print '*'*20
    
    print ex_station+" -> "+ now_station+" : "+str(time)
    
    print ex_station, metro[ex_station]
    print now_station, metro[now_station]
    
    print '-'*20
    
    print metro
    
    print '*'*20
    ex_station = now_station

    
    
    
    
