#include <MMUtilities.h>

MMUtilities::MMUtilities(zmq::context_t* zmqcontext) : DAQUtilities(zmqcontext){

  m_context=zmqcontext;

}

int MMUtilities::ConnectToEndpoints(zmq::socket_t* readrep_sock, std::map<std::string,Store*> &readrep_conns, int read_port_num, zmq::socket_t* write_sock, std::map<std::string,Store*> &write_conns, int write_port_num, zmq::socket_t* mm_sock, std::map<std::string, Store*> &mm_conns, int mm_port_num){
    // it's like UpdateConnections, but rather than connecting to specifically named endpoints,
    // we find all services that aren't middlemen and assume they have associated postgres client endpoints
    // for middlemen, we likewise find their Control service and assume they have a inter-middlemen comms point
    // since we connect to hidden endpoints, we need to already know their port numbers
    
    boost::uuids::uuid m_UUID=boost::uuids::random_generator()();
    long msg_id=0;

    zmq::socket_t Ireceive (*m_context, ZMQ_DEALER);
    Ireceive.connect("inproc://ServiceDiscovery");


    zmq::message_t send(4);
    snprintf ((char *) send.data(), 4 , "%s" ,"All") ;


    if(!Ireceive.send(send)){
    	std::cerr<<"Failed to send 'ALL' query to ServiceDiscovery!"<<std::endl;
    };

    zmq::message_t receive;
    if(!Ireceive.recv(&receive)){
    	std::cerr<<"Failed to receive 'ALL' query from ServiceDiscovery!"<<std::endl;
    };
    std::istringstream iss(static_cast<char*>(receive.data()));

    int size;
    iss>>size;
    
    int num_new_connections=0;

    for(int i=0;i<size;i++){
      
      Store *service = new Store;
      
      zmq::message_t servicem;
      Ireceive.recv(&servicem);
      
      std::istringstream ss(static_cast<char*>(servicem.data()));
      service->JsonParser(ss.str());
      
      std::string type;
      std::string uuid;
      std::string ip;
      std::string store_port="";
      service->Get("msg_value",type);
      service->Get("uuid",uuid);
      service->Get("ip",ip);
      service->Get("remote_port",store_port);
      std::string tmp;
      bool registered=false;
      
      if(type.substr(0,9)!="middleman"){
        // if this isn't a middleman, assume it's a service with a PGClient
        // try to connect to the standard PGClient ports
        
        if(readrep_conns.count(ip)==0){
          registered=true;
          // read queries and responses
          type = "psql_read";
          store_port=std::to_string(read_port_num); // "55555";
          service->Set("msg_value",type);
          service->Set("remote_port",store_port);
          readrep_conns[ip]=service;
          tmp=ip + ":" + store_port;
          tmp="tcp://"+ tmp;
          readrep_sock->connect(tmp.c_str());
          ++num_new_connections;
          
          // write socket is only connected to by the master middleman
          if(write_sock){
            // write queries
            type = "psql_write";
            store_port=std::to_string(write_port_num); // "55556";
            service->Set("msg_value",type);
            service->Set("remote_port",store_port);
            write_conns[ip]=service;
            tmp=ip + ":" + store_port;
            tmp="tcp://"+ tmp;
            write_sock->connect(tmp.c_str());
            ++num_new_connections;
          }
          
        } // else we're already connected to this service
        
      } else {
        // else this is a middleman service. connect to its inter-middleman endpoint
        
        if(mm_conns.count(ip)==0){
          registered=true;
          type="middleman";
          store_port=std::to_string(mm_port_num);
          service->Set("msg_value",type);
          service->Set("remote_port",store_port);
          mm_conns[ip]=service;
          tmp=ip + ":" + store_port;
          tmp="tcp://"+ tmp;
          mm_sock->connect(tmp.c_str());
          ++num_new_connections;
        }
        
      }
      
      if(!registered){
        delete service;
        service=0;
      }
      
    }
    
    return num_new_connections;
}
