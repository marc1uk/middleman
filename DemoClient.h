#ifndef DEMOCLIENT_H
#define DEMOCLIENT_H

#include "ServiceDiscovery.h"
#include "Store.h"
#include "Utilities.h"
#include "Message.h"

#include "zmq.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"

#include <string>
#include <iostream>
#include <map>
#include <cstdlib>   // rand
#include <unistd.h>  // gethostname

#include "errnoname.h"

class DemoClient{
	
	public:
	DemoClient(){};
	bool Initialise(std::string configfile);
	bool Execute();
	bool Finalise();
	
	private:
	
	bool GetResourceUsage(std::string& resource_json);
	bool Log(std::string logmessage, int severity);
	
	Store m_variables;
	
	ServiceDiscovery* service_discovery = nullptr;
	Utilities* utilities = nullptr;
	
	zmq::context_t* context = nullptr;
	
	zmq::socket_t* log_pub_socket = nullptr;
	zmq::socket_t* clt_pub_socket = nullptr;
	zmq::socket_t* clt_dlr_socket = nullptr;
	zmq::socket_t* clt_rep_socket = nullptr;
	
	std::vector<zmq::pollitem_t> in_polls;
	std::vector<zmq::pollitem_t> out_polls;
	
	int max_retries;
	int inpoll_timeout;
	int outpoll_timeout;
	
	// time between write queries
	boost::posix_time::time_duration write_period;
	// time between read queries
	boost::posix_time::time_duration read_period;
	// time between logging queries
	boost::posix_time::time_duration log_period;
	// time between resends if not acknowledged
	boost::posix_time::time_duration resend_period;
	// time between printing info about what we're doing
	boost::posix_time::time_duration print_stats_period;
	// when we last sent a write query
	boost::posix_time::ptime last_write;
	// when we last sent a read query
	boost::posix_time::ptime last_read;
	// when we last printed out stats about what we're doing
	boost::posix_time::ptime last_printout;
	// when we last sent a logging message
	boost::posix_time::ptime last_log;
	
	// messages waiting acknowledgement
	std::map<int, Message> ack_queue;
	
	// general
	int verbosity;
	int get_ok;
	boost::posix_time::time_duration elapsed_time;
	std::string hostname;
	int execute_iterations=0;
	
	// ignore a fraction of acknowledgements received to test resending capability
	double ack_ignore_percent=0;
	
	// ZMQ socket identities - we really only need the reply socket one
	// since that's the one the middleman needs to know to send replies back
	std::string clt_ID;
	
	unsigned int msg_id = 0;
	int log_msgs_sent=0;
	int read_queries_sent=0;
	int write_queries_sent=0;
	int write_queries_resent = 0;
	int read_queries_resent = 0;
	int ok_acks_received=0;
	int read_acks_received=0;
	int write_acks_received=0;
	int err_acks_received=0;
	int other_acks_received=0;
	int unknown_acks_received=0;
	int messages_discarded=0;
	int acks_ignored=0;
};

#endif
