#include "Query.h"

// constructor from elements, given a direct SQL query string
Query::Query(zmq::message_t& client_id_in, zmq::message_t& msg_id_in, zmq::message_t& database_in, zmq::message_t& query_in, unsigned int query_ok_in, std::string response_in){
	client_id.move(&client_id_in);
	message_id.move(&msg_id_in);
	database = std::string(reinterpret_cast<const char*>(database_in.data()));
	query = std::string(reinterpret_cast<const char*>(query_in.data()));
	query_ok=query_ok_in;
	if(response_in!="NULL"){
		response = std::vector<std::string>{response_in};
	}
	retries=0;
	recpt_time = boost::posix_time::microsec_clock::universal_time();
	type = QueryType::direct;
	table="";  // unused for this type
	values=""; // unused for this type
}

// constructor from elements, given a table name and a JSON (used for inserts)
Query::Query(zmq::message_t& client_id_in, zmq::message_t& msg_id_in, zmq::message_t& database_in, zmq::message_t& table_in, zmq::message_t& values_in, unsigned int query_ok_in, std::string response_in){
	client_id.move(&client_id_in);
	message_id.move(&msg_id_in);
	database = std::string(reinterpret_cast<const char*>(database_in.data()));
	query = ""; // unused for this type
	query_ok=query_ok_in;
	if(response_in!="NULL"){
		response = std::vector<std::string>{response_in};
	}
	retries=0;
	recpt_time = boost::posix_time::microsec_clock::universal_time();
	type = QueryType::parameterised;
	table=std::string(reinterpret_cast<const char*>(table_in.data()));
	values=std::string(reinterpret_cast<const char*>(values_in.data()));
}

// copy constructor
Query::Query(const Query& in){
	client_id.copy(&in.client_id);
	message_id.copy(&in.message_id);
	database = in.database;
	type=in.type;
	table=in.table;
	values=in.values;
	query = in.query;
	query_ok = in.query_ok;
	response = in.response;
	retries = in.retries;
	recpt_time = in.recpt_time;
}
