#ifndef QUERY_H
#define QUERY_H

#include <zmq.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>

struct Query{
	Query(zmq::message_t& client_id_in, zmq::message_t& msg_id_in, zmq::message_t& database_in, zmq::message_t& query_in, unsigned int query_ok_in=0, std::string response_in="NULL");
	Query(const Query& in);
	
	zmq::message_t client_id;
	zmq::message_t message_id;
	std::string database;
	std::string query;
	unsigned int query_ok;
	std::vector<std::string> response;
	int retries;
	boost::posix_time::ptime recpt_time;
};

#endif
