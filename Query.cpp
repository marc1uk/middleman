#include "Query.h"

// constructor from elements
Query::Query(zmq::message_t& client_id_in, zmq::message_t& msg_id_in, zmq::message_t& query_in, unsigned int query_ok_in, std::string response_in){
	client_id.move(&client_id_in);
	message_id.move(&msg_id_in);
	query = std::string(reinterpret_cast<const char*>(query_in.data()));
	query_ok=query_ok_in;
	if(response_in!="NULL"){
		response = std::vector<std::string>{response_in};
	}
	retries=0;
	recpt_time = boost::posix_time::microsec_clock::universal_time();
}

// copy constructor
Query::Query(const Query& in){
	client_id.copy(&in.client_id);
	message_id.copy(&in.message_id);
	query = in.query;
	query_ok = in.query_ok;
	response = in.response;
	retries = in.retries;
	recpt_time = in.recpt_time;
}
