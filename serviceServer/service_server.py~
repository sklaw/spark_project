import os.path

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web

import rpyc
time_predictor = rpyc.connect("localhost", 18861)



from tornado.options import define, options
define("port", default=8000, help="run on the given port", type=int)

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
    	print "haha"
        self.render('index.html')

class PoemPageHandler(tornado.web.RequestHandler):
    def post(self):
        year = int(self.get_argument('year'))
        month = int(self.get_argument('month'))
        day = int(self.get_argument('day'))
        hour = int(self.get_argument('hour'))
        minute = int(self.get_argument('minute'))
        station_a = self.get_argument('station_a')
        station_b = self.get_argument('station_b')
        
        #to get the predicting time from time_predictor
        result = time_predictor.root.do_prediction(year, month, day, hour, minute, station_a, station_b);
        
        self.render('poem.html', result=result)
    

if __name__ == '__main__':
    #tornado.options.parse_command_line()
    app = tornado.web.Application(
        handlers=[(r'/', IndexHandler), (r'/poem', PoemPageHandler)],
        template_path=os.path.join(os.path.dirname(__file__), "templates")
    )
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
