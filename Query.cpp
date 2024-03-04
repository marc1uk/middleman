#include "Query.h"

// constructor from elements, given a direct SQL query string
Query::Query(zmq::message_t& client_id_in, zmq::message_t& msg_id_in, const std::string& database_in, const std::string& query_in, uint32_t query_ok_in, std::string response_in){
	client_id.move(&client_id_in);
	message_id.move(&msg_id_in);
	
	database = database_in.substr(0,database_in.find('\0'));
	query = query_in.substr(0,query_in.find('\0'));
	
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

void Query::Print(){
	std::string client(client_id.size(),'\0');
	memcpy((void*)client.c_str(),client_id.data(),client_id.size());
	uint32_t msgid;
	memcpy(&msgid,message_id.data(),sizeof(msgid));
	std::cout<<"query "<<msgid<<" from client "<<client<<": '"
	         <<query<<"', response '";
	for(std::string& apart : response) std::cout<<"["<<apart<<"]";
	std::cout<<"', sent "<<retries<<" times";
	if(retries==0) std::cout<<". message received at ";
	else           std::cout<<" last send attempt at ";
	std::cout<<to_simple_string(recpt_time)<<std::endl;
}
