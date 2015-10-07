import socket
import threading
import SocketServer

spark_streaming_camera_machine_client = None

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        while(1):
            self.data = self.request.recv(1024)
            if not self.data:
                print "{} closed.".format(self.client_address[0])
                break
            print "{} wrote:".format(self.client_address[0])
            print self.data

            spark_streaming_camera_machine_client.sendall(self.data)

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass



if __name__ == "__main__":
    global spark_streaming_camera_machine_client

    host = "localhost"
    port = 9998
    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    soc.bind((host, port))
    soc.listen(1)
    spark_streaming_camera_machine_client, addr = soc.accept()
    
    print 'spark_streaming_camera_machine_client arrived.'
    
    
    
    
    HOST, PORT = "localhost", 19998

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)

    server.serve_forever()

