from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

import os
import math


import rpyc
class MyService(rpyc.Service):
    def on_connect(self):
        # code that runs when a connection is created
        # (to init the serivce, if needed)
        pass

    def on_disconnect(self):
        # code that runs when the connection has already closed
        # (to finalize the service, if needed)
        pass

    def exposed_get_ent_data(self, station_a): # this is an exposed method
    
        #from 2014 10 6
        real_time_data = [(730, 3997), (745, 3647), (800, 3355), (815, 2840), (830, 2341), (845, 2269), (900, 1535), (915, 1634), (930, 1199), (945, 929), (1000, 940), (1015, 740), (1030, 719), (1045, 844), (1100, 701), (1115, 586), (1130, 721), (1145, 844), (1200, 608), (1215, 602), (1230, 710), (1245, 941), (1300, 502), (1315, 628), (1330, 608), (1345, 839), (1400, 527), (1415, 710), (1430, 622), (1445, 662), (1500, 445), (1515, 705), (1530, 635), (1545, 622), (1600, 493), (1615, 594), (1630, 692)]
    
  
        #real_time_data = [(730, 3946), (745, 3780), (800, 3986), (815, 3673), (830, 3296), (845, 2925), (900, 2362), (915, 2166), (930, 1591), (945, 1554), (1000, 1224), (1015, 945), (1030, 969), (1045, 759), (1100, 721), (1115, 879), (1130, 680), (1145, 576), (1200, 728), (1215, 831), (1230, 588), (1245, 582), (1300, 735), (1315, 908), (1330, 527), (1345, 670), (1400, 648), (1415, 858), (1430, 531), (1445, 706), (1500, 604), (1515, 680), (1530, 454), (1545, 676), (1600, 635), (1615, 600), (1630, 488)]
        return real_time_data

    def get_question(self):  # while this method is not exposed
        return "what is the airspeed velocity of an unladen swallow?"
        

count = 0
def fun_3(rdd):
    global count
    rdd.saveAsTextFile(r"/home/sklaw/Desktop/experiment/spark/ex/3/v0/data/stream data/v0/"+str(count))
    count += 1


def threaded_function(arg):
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MyService, port = 18862)
    t.start()


if __name__ == "__main__":
    from threading import Thread
    
    thread = Thread(target = threaded_function, args=(None,))
    thread.start()

 
    conf = SparkConf().setMaster("localhost")

    sc = SparkContext("local[*]", "streaming_part")

    ssc = StreamingContext(sc, 10)
    
    lines = ssc.socketTextStream("localhost", 9999)
    
    lines.foreachRDD(fun_3)
    
   
    ssc.start()
    ssc.awaitTermination()

    

