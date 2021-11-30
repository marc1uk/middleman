#include "LogMsg.h"

// constructor from elements
LogMsg::LogMsg(std::string client_id_in, std::string timestamp_in, unsigned int severity_in, std::string message_in){
	client_id = client_id_in;
	severity = severity_in;
	message = message_in;
	timestamp = timestamp_in;
	
	retries = 0;
	recpt_time = boost::posix_time::microsec_clock::universal_time();
}

// copy constructor
LogMsg::LogMsg(const LogMsg& in){
	client_id = in.client_id;
	severity = in.severity;
	message = in.message;
	timestamp = in.timestamp;
	retries = in.retries;
	recpt_time = in.recpt_time;
}
