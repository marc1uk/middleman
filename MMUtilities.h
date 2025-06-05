#ifndef MM_UTILITIES_H
#define MM_UTILITIES_H

#include <DAQUtilities.h>
#include <errno.h>     // for errno
#include <set>
#include <mutex>

using namespace ToolFramework;

class MMUtilities : public DAQUtilities {

 public:
  
  MMUtilities(zmq::context_t* zmqcontext);

  int ConnectToEndpoints(zmq::socket_t* readrep_sock, std::map<std::string,Store*> &readrep_conns, int read_port_num, std::mutex& readrep_mtx, zmq::socket_t* write_sock, std::map<std::string,Store*> &write_conns, int write_port_num, std::mutex& write_mtx, zmq::socket_t* mm_sock, std::map<std::string, Store*> &mm_conns, int mm_port_num, std::mutex& mm_mtx); ///< Add to standard ports assumed to be associated with all found services.

  bool ClearConnections(zmq::socket_t* sock, std::map<std::string,Store*> &conns, std::mutex& sock_mtx);

private:

  zmq::context_t* m_context;

};




#endif
