#include "Message.h"

Message::Message(int msg_id_in, std::string query_in, bool expect_response_in){
	message_id = msg_id_in;
	query = query_in;
	retries=0;
	last_send_time = boost::posix_time::microsec_clock::universal_time();
	expect_response = expect_response_in;
}

Message::Message(const Message& in){
	message_id      = in.message_id;
	query           = in.query;
	retries         = in.retries;
	last_send_time  = in.last_send_time;
	expect_response = in.expect_response;
}
