#ifndef QUERY_H
#define QUERY_H

#include <zmq.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <string>

enum class QueryType { direct, parameterised };

struct Query{
	Query(zmq::message_t& client_id_in, zmq::message_t& msg_id_in, zmq::message_t& database_in, zmq::message_t& query_in, unsigned int query_ok_in=0, std::string response_in="NULL");
	Query(zmq::message_t& client_id_in, zmq::message_t& msg_id_in, zmq::message_t& database_in, zmq::message_t& table_in, zmq::message_t& values_in, unsigned int query_ok_in=0, std::string response_in="NULL");
	Query(const Query& in);
	
	zmq::message_t client_id;
	zmq::message_t message_id;
	std::string database;
	QueryType type;
	std::string query;
	std::string table;
	std::string values;
	unsigned int query_ok;
	std::vector<std::string> response;
	int retries;
	boost::posix_time::ptime recpt_time;
};

#endif
