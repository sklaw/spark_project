import rpyc

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf().setMaster("localhost")
sc = SparkContext("local[*]", "time_predictor")
sc.setLogLevel("WARN")

import route_with_time
import people_count_with_traffic_warning

import lib.getTwinedDay

class MyService(rpyc.Service):
    def on_connect(self):
        # code that runs when a connection is created
        # (to init the serivce, if needed)
        pass

    def on_disconnect(self):
        # code that runs when the connection has already closed
        # (to finalize the service, if needed)
        pass

    def exposed_get_route_with_time(self, year, month, day, hour, minute, station_a, station_b): # this is an exposed method
        return route_with_time.get_route_with_time(year, month, day, hour, minute, station_a, station_b, sc)

    def exposed_get_people_count_with_traffic_warning(self):
        return people_count_with_traffic_warning.get_people_count_with_traffic_warning(sc)

    def get_question(self):  # while this method is not exposed
        return "what is the airspeed velocity of an unladen swallow?"




def threaded_function(arg):
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port = 18861, protocol_config = {"allow_all_attrs" : True, 'allow_pickle' : True})
    t.start()



if __name__ == "__main__":
    from threading import Thread
    
    lib.getTwinedDay.twined_day_auto_run(sc)
    
    thread = Thread(target = threaded_function, args=(None,))
    thread.start()

    












