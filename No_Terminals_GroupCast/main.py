from pyactor.context import set_context, create_host, serve_forever
from peer import *
from grup import *
from printer import *

URL = 'http://127.0.0.1:1800'

if __name__ == "__main__":

    set_context()
   
    host = create_host(URL)

    grup = host.spawn("grup", Grup)
    printer = host.spawn("printer", Print)
    grup.init_intervals()
    
    membres = []

    for mem in xrange(9):
        aux = host.spawn(str(mem), Peer)
        aux.attach(URL+'/grup', URL+'/printer')
        membres.append(aux)
        sleep(0.5)

    sleep(1)

    i = 0
    for mem in membres:
        mem.multicast('hola'+str(i))
        i +=1
        sleep(0.5)

    sleep(5)

    host.stop_actor('0')
    
    sleep(8)

    for mem in membres:
        mem.multicast('hola'+str(i))
        i += 1
        sleep(0.5)

    serve_forever()
