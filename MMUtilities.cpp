#include <MMUtilities.h>

MMUtilities::MMUtilities(zmq::context_t* zmqcontext) : DAQUtilities(zmqcontext){

  m_context=zmqcontext;

}

const std::set<int> connect_errs{EINVAL,EPROTONOSUPPORT,ENOCOMPATPROTO,ETERM,ENOTSOCK,EMTHREAD};

int MMUtilities::ConnectToEndpoints(zmq::socket_t* readrep_sock, std::map<std::string,Store*> &readrep_conns, int read_port_num, std::mutex& readrep_mtx, zmq::socket_t* write_sock, std::map<std::string,Store*> &write_conns, int write_port_num, std::mutex& write_mtx, zmq::socket_t* mm_sock, std::map<std::string, Store*> &mm_conns, int mm_port_num, std::mutex& mm_mtx){
    // it's like UpdateConnections, but rather than connecting to specifically named endpoints,
    // we find all services that aren't middlemen and assume they have associated postgres client endpoints
    // for middlemen, we likewise find their Control service and assume they have a inter-middlemen comms point
    // since we connect to hidden endpoints, we need to already know their port numbers
    if(readrep_sock==nullptr || write_sock==nullptr || mm_sock==nullptr){
        std::cerr<<"ConnectToEndpoints with null sockets: "<<readrep_sock<<", "<<write_sock<<", "<<mm_sock<<std::endl;
        return 0;
    }
    
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
    
    std::set<std::string> active_endpoints;

    for(int i=0;i<size;i++){
      
      Store *service = new Store;
      
      zmq::message_t servicem;
      Ireceive.recv(&servicem);
      
      std::istringstream ss(static_cast<char*>(servicem.data()));
      service->JsonParser(ss.str());
      
      std::string type;
      std::string ip;
      std::string store_port="";
      service->Get("msg_value",type);
      service->Get("ip",ip);
      service->Get("remote_port",store_port);
      std::string tmp;
      bool registered=false;
      
      active_endpoints.emplace(ip);
      
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
          try {
            readrep_mtx.lock();
            readrep_sock->connect(tmp.c_str());
            readrep_mtx.unlock();
            std::cout<<"MMUtilities::ConnectToEndpoints new connection to read socket "<<tmp<<std::endl;
            ++num_new_connections;
          } catch(zmq::error_t& err){
            std::cerr<<"MMUtilities::ConnectToEndpoints error connecting to read socket "<<tmp<<": "<<err.what()<<std::endl;
          }
          
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
            errno=0;
            try {
              write_mtx.lock();
              write_sock->connect(tmp.c_str()); // no return value but will throw instead!
              write_mtx.unlock();
              std::cout<<"MMUtilities::ConnectToEndpoints new connection to write socket "<<tmp<<std::endl;
              ++num_new_connections;
            } catch(zmq::error_t& err){
              std::cerr<<"MMUtilities::ConnectToEndpoints error connecting to write socket "<<tmp<<": "<<err.what()<<std::endl;
            }
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
          try {
            mm_mtx.lock();
            mm_sock->connect(tmp.c_str());
            mm_mtx.unlock();
            std::cout<<"MMUtilities::ConnectToEndpoints new connection to middleman "<<tmp<<std::endl;
            ++num_new_connections;
          } catch(zmq::error_t& err){
            std::cerr<<"MMUtilities::ConnectToEndpoints error connecting to middleman "<<tmp<<": "<<err.what()<<std::endl;
          }
        }
        
      }
      
      // delete the Store if we're not keeping it
      if(!registered){
        delete service;
        service=0;
      }
      
    } // end loop over services in broadcast
    
    // prune inactive endpoints
    /*
    std::vector<std::map<std::string,Store*>::iterator> to_erase;
    for(std::map<std::string,Store*>::iterator it=readrep_conns.begin(); it!=readrep_conns.end(); ++it){
      if(active_endpoints.count(it->first)==0){
        std::cout<<"Booting inactive endpoint "<<it->first<<std::endl;
        // explicitly disconnect too
        std::string store_port="";
        it->second->Get("remote_port",store_port);
        std::string tmp="tcp://"+ it->first + ":" + store_port;
        try {
          readrep_sock->disconnect(tmp.c_str());
          write_sock->disconnect(tmp.c_str());
        } catch(zmq::error_t& err){
          std::cerr<<"MMUtilities::ConnectToEndpoints error disconnecting from stale socket "<<tmp<<": "<<err.what()<<std::endl;
          // this returns "no such file or directory"?? does it make sense to call disconnect on a sub socket?
          // we should put the two into separate loops
        }
        delete it->second;
        to_erase.push_back(it);
      }
    }
    for(std::map<std::string,Store*>::iterator it : to_erase){
      readrep_conns.erase(it);
    }
    */
    
    return num_new_connections;
}

bool MMUtilities::ClearConnections(zmq::socket_t* sock, std::map<std::string,Store*> &conns, std::mutex& sock_mtx){
    // prune and disconnect from all endpoints
    bool ok=true;
    for(std::map<std::string,Store*>::iterator it=conns.begin(); it!=conns.end(); ++it){
      std::string store_port="";
      it->second->Get("remote_port",store_port);
      std::string tmp="tcp://"+ it->first + ":" + store_port;
      try {
        sock_mtx.lock();
        sock->disconnect(tmp.c_str());
        sock_mtx.unlock();
      } catch(zmq::error_t& err){
        std::cerr<<"MMUtilities::ClearConnections error disconnecting from client "<<tmp<<": "<<err.what()<<std::endl;
        ok = false;
      }
      delete it->second;
    }
    conns.clear();
    return ok;
}
