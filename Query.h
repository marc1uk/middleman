#ifndef QUERY_H
#define QUERY_H

#include <zmq.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>

struct Query{
	Query(zmq::message_t& client_id_in, zmq::message_t& msg_id_in, const std::string& database_in, const std::string& query_in, uint32_t query_ok_in=0, std::string response_in="NULL");
	Query(const Query& in);
	void Print();
	
	zmq::message_t client_id;
	zmq::message_t message_id;
	std::string database;
	std::string query;
	uint32_t query_ok;
	std::vector<std::string> response;
	int retries;
	boost::posix_time::ptime recpt_time;
};

#endif
