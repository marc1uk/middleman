#include "Query.h"

// constructor from elements, given a direct SQL query string
Query::Query(zmq::message_t& client_id_in, zmq::message_t& msg_id_in, zmq::message_t& database_in, zmq::message_t& query_in, unsigned int query_ok_in, std::string response_in){
	client_id.move(&client_id_in);
	message_id.move(&msg_id_in);
	
	// NOPE reinterpret_cast and copy construction doesn't work
	//database = reinterpret_cast<const char*>(database_in.data());
	// XXX instead use memcpy and trim
	database.resize(database_in.size(),'\0');
	memcpy((void*)database.data(),database_in.data(),database_in.size());
	database = database.substr(0,database.find('\0'));
	
	//query = reinterpret_cast<const char*>(query_in.data());
	query.resize(query_in.size(),'\0');
	memcpy((void*)query.data(),query_in.data(),query_in.size());
	query = query.substr(0,query.find('\0'));
	
	//std::cout<<"building query for db: '"<<database<<"', query_string: '"<<query<<"'"<<std::endl;
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
	database = in.database;
	query = in.query;
	query_ok = in.query_ok;
	response = in.response;
	retries = in.retries;
	recpt_time = in.recpt_time;
}
