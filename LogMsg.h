#ifndef LOGMSG_H
#define LOGMSG_H

#include <zmq.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>

struct LogMsg{
	LogMsg(std::string client_id_in, std::string timestamp_in, unsigned int severity_in, std::string message_in);
	LogMsg(const LogMsg& in);
	
	std::string client_id;
	std::string message;
	unsigned int severity;
	std::string timestamp;
	int retries;
	boost::posix_time::ptime recpt_time;
};

#endif
