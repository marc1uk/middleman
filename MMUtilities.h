#ifndef MM_UTILITIES_H
#define MM_UTILITIES_H

#include <DAQUtilities.h>

using namespace ToolFramework;

class MMUtilities : public DAQUtilities {

 public:
  
  MMUtilities(zmq::context_t* zmqcontext);

  int ConnectToEndpoints(zmq::socket_t* readrep_sock, std::map<std::string,Store*> &readrep_conns, int read_port_num, zmq::socket_t* write_sock, std::map<std::string,Store*> &write_conns, int write_port_num, zmq::socket_t* mm_sock, std::map<std::string, Store*> &mm_conns, int mm_port_num); ///< Add to standard ports assumed to be associated with all found services.

private:

  zmq::context_t* m_context;

};




#endif
