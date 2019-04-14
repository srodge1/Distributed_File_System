#!/usr/bin/env python

import glob
import sys
import os
import socket as _s
import hashlib as _hl
import threading as _th
import logging
logging.basicConfig(level=logging.DEBUG)
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
import chord.ttypes as _tt

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


def create_pred_client(serv_node):
    '''
    follows up the findPred() function if other server is to be contacted for finding the suc_node
    input : NodeID serv_node, sha256(file_name) file_id
    output: NodeID pred_node
    '''
    #print "\nin create_pred_client : "+str(serv_node)+"\n"
    transport = TSocket.TSocket(serv_node.ip, serv_node.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = FileStore.Client(protocol)
    transport.open()

    return client

def create_suc_client(serv_node):
    '''
    follows up the findSucc() function if other server is to be contacted for finding the next active node
    input : NodeID serv_node
    output: NodeID suc_node
    '''
    transport = TSocket.TSocket(serv_node.ip, serv_node.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol  = TBinaryProtocol.TBinaryProtocol(transport)
    client    = FileStore.Client(protocol)
    transport.open()

    return client

class FileStoreHandler:
    def __init__(self):
        self.dht_files  = {}
        self.node_list  = []
        # Creating NodeID object of this server, i.e. node in the chord ring
        self.node       = _tt.NodeID()
        # Use _s.getaddrinfo() for ipv4/6 dual stack support. _s.gethostname support ipv4 only
        self.node.ip    = _s.gethostbyname(_s.gethostname())
        # keeping sys.srgv[1] as string was throwing internal error, since idl was not able to convert it to int
        self.node.port  = int(sys.argv[1])
        self.node.id    = _hl.sha256(str(self.node.ip)+':'+str(self.node.port)).hexdigest()
        print(self.node)

    def writeFile(self, rfile):
        '''
        Writes the file to the server dictionary, such that filename is key and RFile object is data.
        input : RFile rfile
        '''
        # contentHash is useful in error detecting no use here.
        # saving dict{file_name_hash : RFile object} this will be usefull in locating files efficiently
        file_name_hash  = _hl.sha256(rfile.meta.filename).hexdigest()
        test_node       = self.findSucc(file_name_hash)
        #print("\nin server write : test_node"+str(test_node)+"self.node : "+str(self.node))
        #print(test_node == self.node)
        if test_node == self.node:
            if file_name_hash not in self.dht_files:
                #if file already not in memory, save it with version = 0
                meta        = _tt.RFileMetadata()
                meta        = rfile.meta
                meta.version= 0
                meta.contentHash   = _hl.sha256(rfile.content).hexdigest()
                w_file      = _tt.RFile()
                w_file.meta = meta
                w_file.content = rfile.content
                self.dht_files[file_name_hash] = w_file

            else:
                # else if file already in memory version += 1 amd replace the content
                self.dht_files[file_name_hash].meta.version     += 1
                self.dht_files[file_name_hash].meta.content     = rfile.content
                # requirements pdf doesn't say this. mention in the documentation
                self.dht_files[file_name_hash].meta.contentHash = _hl.sha256(rfile.content).hexdigest()

        else:
            raise _tt.SystemException(message="Server : "+str(self.node)+" doesn't own this file id.")

    def readFile(self, file_name):
        '''
        Reads file from the server dictionary.
        input : string file_name
        output: RFile file_name
        '''
        # inside the if rfile exists on this node block.
        file_name_hash  = _hl.sha256(file_name).hexdigest()
        test_node       = self.findSucc(file_name_hash)
        # __eq__ is implemented in init so it should work
        if test_node == self.node:
            if file_name_hash not in self.dht_files:
                ex  = _tt.SystemException(message="No such file with this filename exists on this server!"+
                        str(self.node))
                raise ex

            return self.dht_files[file_name_hash]

        raise _tt.SystemException(message="Server : "+str(self.node)+" doesn't own this file.")

    def setFingertable(self, node_list):
        '''
        Sets server's finger table to node_list.
        input : list<NodeID> node_list
        '''
        try:
            self.node_list = node_list
            #print self.node_list
        except:
            ex  = _tt.SystemException(message="Could not set finger table for node : "+
                    str(self.node))
            raise ex
        # If you want to have a better look at fingertables of all nodes uncomment the following if else block
        if 'nodes.txt' not in os.listdir(os.getcwd()):
            test_file = open('nodes.txt', 'w+')
            test_file.write("\n\n----begin----\n\n"+str(self.node_list)+"\n\n----END---\n\n")
            test_file.close()
        else:
            test_file = open('nodes.txt', 'a')
            test_file.write("\n\n----"+str(self.node)+"----\n\n"+str(self.node_list)+"\n\n----END---\n\n")
            test_file.close()

    def findSucc(self, file_id):
        '''
        returns which node in the chord is associated with given identifier.
        input : sha256(file_name) file_id
        output: NodeID n
        '''
        if file_id in self.dht_files:
            return self.node

        temp_node   = self.findPred(file_id)
        #print "\nTemp Node : "+str(temp_node)+"\n"
        if temp_node == self.node and file_id < self.node.id:
            if self.node_list[0].id > self.node.id:
                return self.node

        return create_suc_client(temp_node).getNodeSucc()

    def findPred(self, file_id):
        '''
        returns the preceding node of the node which keeps the file_id.
        input : sha256(file_name) file_id
        output: NodeID n
        '''
        if not self.node_list:
            raise _tt.SystemException(message="Fingertable not set for the server : "+str(self.node))
        # comparison between strings work(sha256().hexdigest()), check again with more examples.
        #print("\nin pred : "+str(self.node)+"\n\n")
        if self.node.id > self.node_list[0].id:
            if file_id > self.node.id or file_id <= self.node_list[0].id:
                return self.node
        # else can be removed but eases the readding.
        else:
            if file_id > self.node.id and file_id <= self.node_list[0].id:
                return self.node

        pred_node = None
        for i in reversed(range(-256, 0, 1)):
            if self.node_list[i].id > self.node.id:
                if self.node_list[i].id > self.node.id and self.node_list[i].id <= file_id:
                    pred_node = self.node_list[i]
                    break

            else:
                if file_id > self.node_list[i].id and file_id < self.node.id:
                    pred_node = self.node_list[i]
                    break
        if not pred_node:
            return self.node
        #print("\n\n=========After calculating pred_node===="+str(pred_node)+"\n\n")
        pred_node = create_pred_client(pred_node).findPred(file_id)

        return pred_node

    def getNodeSucc(self):
        '''
        returns the next active node in the DHT chord.
        output: NodeID n
        '''
        if not self.node_list:
            raise _tt.SystemException(message="Fingertable not set for the server : "+str(self.node))
        # this should work but the printed node list didn't seem like it
        return self.node_list[0]

if __name__ == '__main__':
    handler = FileStoreHandler()
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
