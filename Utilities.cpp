#include <Utilities.h>

Utilities::Utilities(zmq::context_t* zmqcontext){ 
  context=zmqcontext;
  Threads.clear();
}

bool Utilities::AddService(std::string ServiceName, unsigned int port, bool StatusQuery){

  zmq::socket_t Ireceive (*context, ZMQ_PUSH);
  Ireceive.connect("inproc://ServicePublish");

  boost::uuids::uuid m_UUID;
  m_UUID = boost::uuids::random_generator()();

  std::stringstream test;
  test<<"Add "<< ServiceName <<" "<<m_UUID<<" "<<port<<" "<<((int)StatusQuery) ;

  zmq::message_t send(test.str().length()+1);
  snprintf ((char *) send.data(), test.str().length()+1 , "%s" ,test.str().c_str()) ;
 
  return Ireceive.send(send);


}


bool Utilities::RemoveService(std::string ServiceName){

  zmq::socket_t Ireceive (*context, ZMQ_PUSH);
  Ireceive.connect("inproc://ServicePublish");

  std::stringstream test;
  test<<"Delete "<< ServiceName << " ";
  zmq::message_t send(test.str().length()+1);
  snprintf ((char *) send.data(), test.str().length()+1 , "%s" ,test.str().c_str()) ;

  return Ireceive.send(send);

}

int Utilities::UpdateConnections(std::string ServiceName, zmq::socket_t* sock, std::map<std::string,Store*> &connections, std::string port){

    boost::uuids::uuid m_UUID=boost::uuids::random_generator()();
    long msg_id=0;

    zmq::socket_t Ireceive (*context, ZMQ_DEALER);
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
      if(port==""){
          tmp=ip + ":" + store_port;
      } else {
          tmp=ip + ":" + port;
      }

      //if(type == ServiceName && connections.count(uuid)==0){
      if(type == ServiceName && connections.count(tmp)==0){
	connections[tmp]=service;
	//std::string ip;
	//std::string port;
	//service->Get("ip",ip);
	//service->Get("remote_port",port);
	tmp="tcp://"+ tmp;
	sock->connect(tmp.c_str());
      }
      else{
	delete service;
	service=0;
      }


    }

    return connections.size();
  }

int Utilities::ConnectToEndpoints(zmq::socket_t* readrep_sock, std::map<std::string,Store*> &readrep_conns, int read_port_num, zmq::socket_t* write_sock, std::map<std::string,Store*> &write_conns, int write_port_num, zmq::socket_t* mm_sock, std::map<std::string, Store*> &mm_conns, int mm_port_num){
    // it's like UpdateConnections, but rather than connecting to specifically named endpoints,
    // we find all services that aren't middlemen and assume they have associated postgres client endpoints
    // for middlemen, we likewise find their Control service and assume they have a inter-middlemen comms point
    // since we connect to hidden endpoints, we need to already know their port numbers
    
    boost::uuids::uuid m_UUID=boost::uuids::random_generator()();
    long msg_id=0;

    zmq::socket_t Ireceive (*context, ZMQ_DEALER);
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

Thread_args* Utilities::CreateThread(std::string ThreadName,  void (*func)(Thread_args*), Thread_args* args){
  
  if(Threads.count(ThreadName)==0){
    
    if(args==0) args = new Thread_args();  
    
    args->context=context;
    args->ThreadName=ThreadName;
    args->func=func;
    args->running=true;
    
    pthread_create(&(args->thread), NULL, Utilities::Thread, args);
    
    args->sock=0;
    Threads[ThreadName]=args;
    
}

  else args=0;

  return args;
  
}


Thread_args* Utilities::CreateThread(std::string ThreadName,  void (*func)(std::string)){
  Thread_args *args =0;
  
  if(Threads.count(ThreadName)==0){
    
    args = new Thread_args(context, ThreadName, func);
    pthread_create(&(args->thread), NULL, Utilities::String_Thread, args);
    args->sock=0;
    args->running=true;
    Threads[ThreadName]=args;
  }
  
  return args;
}                    

void *Utilities::String_Thread(void *arg){
  

    Thread_args *args = static_cast<Thread_args *>(arg);

    zmq::socket_t IThread(*(args->context), ZMQ_PAIR);
    /// need to subscribe
    std::stringstream tmp;
    tmp<<"inproc://"<<args->ThreadName;
    IThread.bind(tmp.str().c_str());


    zmq::pollitem_t initems[] = {
      {IThread, 0, ZMQ_POLLIN, 0}};

    args->running = true;

    while(!args->kill){
      if(args->running){
	
	std::string command="";
	
	zmq::poll(&initems[0], 1, 0);
	
	if ((initems[0].revents & ZMQ_POLLIN)){
	  
	  zmq::message_t message;
	  IThread.recv(&message);
	  command=std::string(static_cast<char *>(message.data()));
  	  
	}
	
	args->func_with_string(command);
      }

      else usleep(100);

    }
    
    pthread_exit(NULL);
  }

void *Utilities::Thread(void *arg){
  
  Thread_args *args = static_cast<Thread_args *>(arg);

  while (!args->kill){
    
    if(args->running) args->func(args );
    else usleep(100);
  
  }
  
  pthread_exit(NULL);

}

bool Utilities::MessageThread(Thread_args* args, std::string Message, bool block){ 

  bool ret=false; 

  if(args){
    
    if(!args->sock){
      
      args->sock = new zmq::socket_t(*(args->context), ZMQ_PAIR);
      std::stringstream tmp;
      tmp<<"inproc://"<<args->ThreadName;
      args->sock->connect(tmp.str().c_str());
      
    }
    
    zmq::message_t msg(Message.length()+1);
    snprintf((char *)msg.data(), Message.length()+1, "%s", Message.c_str());
   
    if(block) ret=args->sock->send(msg);
    else ret=args->sock->send(msg, ZMQ_NOBLOCK);
    
  }
  
  return ret;
  
}

bool Utilities::MessageThread(std::string ThreadName, std::string Message, bool block){

  return MessageThread(Threads[ThreadName],Message,block);
}

bool Utilities::KillThread(Thread_args* &args){
  
  bool ret=false;
  
  if(args){
    
    args->running=false;
    args->kill=true;
    
    pthread_join(args->thread, NULL);
    //delete args;
    //args=0;
    
    
  }
  
  return ret;   
  
}

bool Utilities::KillThread(std::string ThreadName){
  
  return KillThread(Threads[ThreadName]);

}

