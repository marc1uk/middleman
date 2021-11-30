#ifndef MESSAGE_H
#define MESSAGE_H

#include <zmq.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>

struct Message{
	Message(int msg_id_in, std::string query_in, bool expect_response_in);
	Message(const Message& in);
	
	int message_id;
	std::string query;
	int retries;
	boost::posix_time::ptime last_send_time;
	bool expect_response;
};

#endif
