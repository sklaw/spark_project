from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf().setMaster("localhost")
sc = SparkContext("local[*]", "data_transformer")
import oct2py
oc = oct2py.Oct2Py()

def pre_process(line):
    global mapping
    elem_list = line.split(",")
    return ((int(elem_list[1])), (int(elem_list[2])))
    
def draw(path):
    file_data = sc.textFile(path)
    data = file_data.map(pre_process).reduceByKey(lambda a,b: a+b).collect()
    data = sorted(data, key=lambda x: x[0])
    
    print data
    
    oc.figure(path.split('/')[-1].replace('-',''))
    oc.plot([i[0] for i in data], [i[1] for i in data])

if __name__ == "__main__":
    draw(r"/home/sklaw/Desktop/experiment/spark/ex/3/v1/timePredictor/data/v0/Addison Road/1/2014-10-6")
    draw(r"/home/sklaw/Desktop/experiment/spark/ex/3/v1/timePredictor/data/v0/Addison Road/1/2014-10-13")
    draw(r"/home/sklaw/Desktop/experiment/spark/ex/3/v1/timePredictor/data/v0/Addison Road/1/2014-10-20")
    draw(r"/home/sklaw/Desktop/experiment/spark/ex/3/v1/timePredictor/data/v0/Addison Road/1/2014-10-27")
    
    raw_input()
