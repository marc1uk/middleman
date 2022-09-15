/* vim:set noexpandtab tabstop=4 wrap filetype=cpp */
#include "DemoClient.h"
#include <errno.h>

bool DemoClient::Initialise(std::string configfile){
	
	/*               Retrieve Configs            */
	/* ----------------------------------------- */
	
	// configuration options can be parsed via a Store class
	m_variables.Initialise(configfile);
	
	
	/*            General Variables              */
	/* ----------------------------------------- */
	verbosity = 3;
	max_retries = 3;
	m_variables.Get("verbosity",verbosity);
	m_variables.Get("max_retries",max_retries);
	m_variables.Get("ack_ignore_percent",ack_ignore_percent);
	
	// we have three zmq sockets:
	// 1. [PUB]    one for sending write queries to all listeners (the master)
	// 2. [DEALER] one for sending read queries round-robin and receving responses
	// 3. [PUB]    one for sending log messages
	
	// specify the ports everything talks/listens on
	int clt_pub_port = 77778;   // for sending write queries
	int clt_dlr_port = 77777;   // for sending read queries
	int log_pub_port = 77776;   // for sending log messages
	// socket timeouts, so nothing blocks indefinitely
	int clt_pub_socket_timeout=500;
	int clt_dlr_socket_timeout=500;  // both send and receive
	int log_pub_socket_timeout=500;
	
	// poll timeouts - units are milliseconds
	inpoll_timeout=500;
	outpoll_timeout=500;
	
	// Update with user-specified values.
	m_variables.Get("clt_pub_port",clt_pub_port);
	m_variables.Get("clt_dlr_port",clt_dlr_port);
	m_variables.Get("log_pub_port",log_pub_port);
	m_variables.Get("clt_pub_socket_timeout",clt_pub_socket_timeout);
	m_variables.Get("clt_dlr_socket_timeout",clt_dlr_socket_timeout);
	m_variables.Get("log_pub_socket_timeout",log_pub_socket_timeout);
	m_variables.Get("inpoll_timeout",inpoll_timeout);
	m_variables.Get("outpoll_timeout",outpoll_timeout);
	
	// open a zmq context
	int context_io_threads = 1;
	context = new zmq::context_t(context_io_threads);
	
	/*               Service Discovery           */
	/* ----------------------------------------- */
	
	// Use a service discovery class to broadcast messages signalling our presence.
	// The middlemen can pick up on these broadcasts and connect to us to receive data.
	// Read service discovery configs into a Store
	std::string service_discovery_config;
	m_variables.Get("service_discovery_config", service_discovery_config);
	Store service_discovery_configstore;
	service_discovery_configstore.Initialise(service_discovery_config);
	
	// we do wish to send broadcasts advertising our services
	bool send_broadcasts = true;
	// the ServiceDiscovery class can also listen for other service providers, but we don't need to.
	bool rcv_broadcasts = false;
	
	// multicast address and port to broadcast on. must match the middleman.
	std::string broadcast_address = "239.192.1.1";
	int broadcast_port = 5000;
	service_discovery_configstore.Set("broadcast_address",broadcast_address);
	service_discovery_configstore.Set("broadcast_port",broadcast_port);
	
	// how frequently to broadcast
	int broadcast_period_sec = 5;
	service_discovery_configstore.Set("broadcast_period",broadcast_period_sec);
	
	// a unique identifier for us. this is used by the ServiceDiscovery listener thread,
	// which maintains a map of services it's recently heard about, for which this is the key.
	boost::uuids::uuid client_id = boost::uuids::random_generator()();
	
	// a service name. The Utilities class then has a helper function that will connect
	// a given zmq socket to all broadcasters with a given service name, which allows
	// the middlemen to connect a receive_read_query socket to all discovered clients'
	// send_read_query sockets, for example. In the constructor the service name is the
	// name of the service used for RemoteControl (which we will not be using).
	std::string client_name = "DemoClient";
	service_discovery_configstore.Get("client_name",client_name);
	
	// the ServiceDiscovery class will inherently advertise the presence of a remote control port,
	// and will attempt to check the status of that service on each broadcast by querying
	// localhost:remote_control_port. Unless we implement a listener to respond to those messages,
	// the zmq poll will timeout and the reported Status will just be "Status".
	// (the RemoteControl Service is normally implemented as part of ToolDAQ)
	int remote_control_port = 24011;
	
	// construct the ServiceDiscovery instance.
	// it'll handle the broadcasting in a separate thread - we don't need to do anything else.
	service_discovery = new ServiceDiscovery(send_broadcasts, rcv_broadcasts, remote_control_port,
	                                         broadcast_address, broadcast_port, context, client_id,
	                                         client_name, broadcast_period_sec);
	
	/*                  ZMQ Setup                */
	/* ----------------------------------------- */
	// to send replies the middleman must know who to send them to.
	// for read queries, the receiving router socket will append the ZMQ_IDENTITY of the sender
	// which can be given to the sending router socket to identify the recipient.
	// BUT the default ZMQ_IDENTITY of a socket is empty! We must set it ourselves to be useful!
	// for write queries we ALSO need to manually insert the ZMQ_IDENTITY into the written message,
	// because the receiving sub socket does not do this automaticaly.
	
	// using 'getsockopt(ZMQ_IDENTITY)' without setting it first produces an empty string,
	// so seems to need to set it manually to be able to know what the ID is, and
	// insert it into the write queries.
	boost::uuids::uuid u = boost::uuids::random_generator()();
	clt_ID = boost::uuids::to_string(u);
	clt_ID += '\0';
	
	// socket to publish write queries
	// -------------------------------
	clt_pub_socket = new zmq::socket_t(*context, ZMQ_PUB);
	clt_pub_socket->setsockopt(ZMQ_SNDTIMEO, clt_pub_socket_timeout);
	clt_pub_socket->setsockopt(ZMQ_LINGER, 10);
	zmq::pollitem_t clt_pub_socket_pollout= zmq::pollitem_t{*clt_pub_socket,0,ZMQ_POLLOUT,0};
	clt_pub_socket->bind(std::string("tcp://*:")+std::to_string(clt_pub_port));
	
	// socket to deal read queries and receive responses
	// -------------------------------------------------
	clt_dlr_socket = new zmq::socket_t(*context, ZMQ_DEALER);
	clt_dlr_socket->setsockopt(ZMQ_SNDTIMEO, clt_dlr_socket_timeout);
	clt_dlr_socket->setsockopt(ZMQ_RCVTIMEO, clt_dlr_socket_timeout);
	clt_dlr_socket->setsockopt(ZMQ_LINGER, 10);
	// set identity property of the client so receiving ROUTER can access it.
	clt_dlr_socket->setsockopt(ZMQ_IDENTITY, clt_ID.c_str(), clt_ID.length());
	// double check by getting it back
	uint8_t the_ID[255];
	memset(&the_ID[0],0,255);
	size_t ID_size;
	std::cout<<"Reading back"<<std::endl;
	clt_dlr_socket->getsockopt(ZMQ_IDENTITY, the_ID, &ID_size);
	std::cout<<"Retrieved ID was '"<<the_ID<<"' of length "<<ID_size<<std::endl;
	std::cout<<"binary comparison returned "<<(memcmp(&the_ID[0], clt_ID.c_str(), ID_size))<<std::endl;
	
	zmq::pollitem_t clt_dlr_socket_pollin = zmq::pollitem_t{*clt_dlr_socket,0,ZMQ_POLLIN,0};
	zmq::pollitem_t clt_dlr_socket_pollout = zmq::pollitem_t{*clt_dlr_socket,0,ZMQ_POLLOUT,0};
	clt_dlr_socket->bind(std::string("tcp://*:")+std::to_string(clt_dlr_port));
	
	// socket to send log messages
	// ---------------------------
	log_pub_socket = new zmq::socket_t(*context, ZMQ_PUB);
	log_pub_socket->setsockopt(ZMQ_SNDTIMEO, log_pub_socket_timeout);
	// don't linger too long, it looks like the program crashed.
	log_pub_socket->setsockopt(ZMQ_LINGER, 10);
	log_pub_socket->bind(std::string("tcp://*:")+std::to_string(log_pub_port));
	zmq::pollitem_t log_pub_socket_pollout= zmq::pollitem_t{*log_pub_socket,0,ZMQ_POLLOUT,0};
	
	// bundle the polls together so we can do all of them at once
	in_polls = std::vector<zmq::pollitem_t>{clt_dlr_socket_pollin};
	out_polls = std::vector<zmq::pollitem_t>{clt_pub_socket_pollout,
	                                         clt_dlr_socket_pollout,
	                                         log_pub_socket_pollout};
	
	/*             Register Services             */
	/* ----------------------------------------- */
	
	// to register our query and response ports with the ServiceDiscovery class
	// we can make our lives a little easier by using a Utilities class
	utilities = new Utilities(context);
	
	// we can now register the client sockets with the following:
	utilities->AddService("psql_write", clt_pub_port);
	utilities->AddService("psql_read",  clt_dlr_port);
	utilities->AddService("logging",  log_pub_port);
	
	// if we need to we can remove it from the broadcasts with e.g:
	//utilities->RemoveService("psql_write");
	
	// on the middleman, we can connect a listener to the clients with:
	//utilities->UpdateConnections("psql_write", clt_pub_socket, connections);
	// where 'connections' is a map of the connection string to connection details,
	// that must be retained between calls - it's used to identify new clients.
	
	/*                Time Tracking              */
	/* ----------------------------------------- */
	
	// we'll send out write requests and read requests at defined intervals.
	// To handle tracking of the intervals we'll use boost timestamps
	
	// time between write queries
	int write_period_ms = 1000;
	// time between read queries
	int read_period_ms = 1000;
	// time between logging messages
	int log_period_ms = 1000;
	// time to wait between resend attempts if not ack'd
	int resend_period_ms = 5000;
	// how often to print out stats on what we're sending
	int print_stats_period_ms = 1000;
	
	// Update with user-specified values.
	m_variables.Get("write_period_ms",write_period_ms);
	m_variables.Get("read_period_ms",read_period_ms);
	m_variables.Get("log_period_ms",log_period_ms);
	m_variables.Get("resend_period_ms",resend_period_ms);
	m_variables.Get("print_stats_period_ms",print_stats_period_ms);
	
	// convert times to boost for easy handling
	write_period = boost::posix_time::milliseconds(write_period_ms);
	read_period = boost::posix_time::milliseconds(read_period_ms);
	log_period = boost::posix_time::milliseconds(log_period_ms);
	resend_period = boost::posix_time::milliseconds(resend_period_ms);
	print_stats_period = boost::posix_time::milliseconds(print_stats_period_ms);
	
	// initialise 'last send' times
	last_write = boost::posix_time::microsec_clock::universal_time();
	last_read = boost::posix_time::microsec_clock::universal_time();
	last_log = boost::posix_time::microsec_clock::universal_time();
	last_printout = boost::posix_time::microsec_clock::universal_time();
	
	// get the hostname of this machine for monitoring stats
	char buf[255];
	get_ok = gethostname(buf, 255);
	if(get_ok!=0){
		std::cerr<<"Error getting hostname!"<<std::endl;
		perror("gethostname: ");
		hostname = "unknown";
	} else {
		hostname = std::string(buf);
	}
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool DemoClient::Execute(){
	++execute_iterations;
	
	// start with the reads.
	// poll the input sockets
	get_ok = zmq::poll(in_polls.data(), in_polls.size(), inpoll_timeout);
	if(get_ok<0){
		std::cerr<<"Warning! ReceiveSQL error polling input sockets; have they closed?"<<std::endl;
	}
	
	if(in_polls.at(0).revents & ZMQ_POLLIN){
		// receive incoming message.
		// it may be an acknowledgement...
		int message_id_rcvd=-1;
		int ack_result=-1;  // (it'll be a bool, for now, so 0 or 1)
		// ... or it may be an sql query result
		std::vector<zmq::message_t> query_response;
		
		// responses are expected to be 2+ part messages
		// 1. the message ID, used by the client to match to the message it sent
		// 2. the response code, to signal errors
		// 3.... the SQL query results, if any. Each row is returned in a new message part.
		zmq::message_t message_id;
		zmq::message_t resp_code;
		
		// get first part (message id)
		if(clt_dlr_socket->recv(&message_id) && message_id.more()){
			message_id_rcvd = *reinterpret_cast<int*>(message_id.data());
			
			// get 2nd part (response code)
			if(clt_dlr_socket->recv(&resp_code) && resp_code.more()){
				
				// read all futher parts - the SQL query result.
				// read out all further parts into a vector
				query_response.resize(1);
				while(clt_dlr_socket->recv(&query_response.back()) && query_response.back().more()){
					query_response.resize(query_response.size()+1);
				}
				
				// check we stopped reading because there were no more parts
				if(query_response.back().more()){
					std::cerr<<"Error receiving reply part "<<(query_response.size()+2)
					         <<"!"<<std::endl;
				}
				
				// check we recognise the acknowledged message id
				if(ack_queue.count(message_id_rcvd)){
					
					// check whether the query succeeded
					ack_result = *reinterpret_cast<int*>(resp_code.data());
					if(ack_result==1){
						std::cout<<"Message "<<message_id_rcvd<<" acknowledged ok!"<<std::endl;
						++ok_acks_received;
					} else if(ack_result==0){
						std::cerr<<"Message "<<message_id_rcvd<<" acknowledged failure!"<<std::endl;
						++err_acks_received;
					} else {
						std::cerr<<"Message "<<message_id_rcvd<<" unknown acknowledgement code "
							     <<ack_result<<"!"<<std::endl;
						++other_acks_received;
					}
					
					if(!ack_queue.at(message_id_rcvd).expect_response && ack_result==1){
						// if ack_result==0 (query failed) response contains an error message
						std::cerr<<"Was not expecting a response to this query...!"<<std::endl;
					}
					
					// check the returned query results are sensible
					// first get the query to which this is a response
					Message req_msg = ack_queue.at(message_id_rcvd);
					std::string qry = req_msg.query;
					
					// "validate" ... for now just print them? FIXME this is too verbose
					/*
					std::cout<<"Response to query: '"<<qry<<"' was: '\n";
					for(zmq::message_t& rep_part : query_response){
						std::string rep(reinterpret_cast<const char*>(rep_part.data()));
						std::cout<<rep<<"\n";
					}
					std::cout<<"'"<<std::endl;
					*/
					
					// in any case, remove from the queue of messages waiting for acknowledgement
					// ... or in some cases, just to test the re-sending capability, don't!
					if((rand() % 100) < ack_ignore_percent){
						++acks_ignored;
					} else {
						ack_queue.erase(message_id_rcvd);
					}
					
				} else {
					std::cerr<<"acknowledgement for unknown message id "
					         <<message_id_rcvd<<std::endl;
					++unknown_acks_received;
				}
				
			} else if(!resp_code.more()){
				// else only 2 part message - just an acknowledgement of processing, no query results
				
				// check we recognise the acknowledged message id
				if(ack_queue.count(message_id_rcvd)){
					
					// check whether the query succeeded
					ack_result = *reinterpret_cast<int*>(resp_code.data());
					if(ack_result==1){
						std::cout<<"Message "<<message_id_rcvd<<" acknowledged ok!"<<std::endl;
						++ok_acks_received;
					} else if(ack_result==0){
						std::cerr<<"Message "<<message_id_rcvd<<" acknowledged failure!"<<std::endl;
						++err_acks_received;
					} else {
						std::cerr<<"Message "<<message_id_rcvd<<" unknown acknowledgement code "
							     <<ack_result<<"!"<<std::endl;
						++other_acks_received;
					}
					
					// check if this was a read query that was expecting a response
					if(ack_queue.at(message_id_rcvd).expect_response){
						std::cout<<"no SQL response - perhaps no rows matched?"<<std::endl;
					}
					
					// in any case, remove it from the list waiting for acknowledgement.
					// ... probably. But to test the re-send functionality, occasionally don't!
					if((rand() % 100) < ack_ignore_percent){
						++acks_ignored;
					} else {
						ack_queue.erase(message_id_rcvd);
					}
					
				} else {
					std::cerr<<"acknowledgement for unknown message id "
					         <<message_id_rcvd<<std::endl;
					++unknown_acks_received;
				}
				
			} else {
				std::cerr<<"Error receiving reply part 1!"<<std::endl;
			}
			
		} else if(!message_id.more()){
			// too few parts! Where's the message?
			std::cerr<<"Too few parts (1) in response message!"<<std::endl;
		} else {
			std::cerr<<"Error receiving reply part 0!"<<std::endl;
		}
		
	}  // otherwise no incoming messages
/*
else {
std::cout<<"no incoming messages to read"<<std::endl;
}
*/
	
	// move on to writes
	// poll the output sockets
	get_ok = zmq::poll(out_polls.data(), out_polls.size(), outpoll_timeout);
	if(get_ok<0){
		std::cerr<<"Warning! ReceiveSQL error polling output sockets; have they closed?"<<std::endl;
	}
	
	// first write queries - check if we have a listener
	if(out_polls.at(0).revents & ZMQ_POLLOUT){
		
		// check if we're due to send a write query
		elapsed_time = boost::posix_time::microsec_clock::universal_time() - last_write;
		elapsed_time = write_period - elapsed_time;
		
		if(elapsed_time.is_negative()){
			// time to send a write query.
			
			// get resource usage as data to insert into the database
			std::string resource_json;
			get_ok = GetResourceUsage(resource_json);
			if(not get_ok){
				std::cerr<<"Error getting resource usage stats!"<<std::endl;
			}
			
			// format zmq message
			
			// first the sender id
			// FIXME should this not be size+1 rather than length?
			zmq::message_t host_id_zmq(clt_ID.length());
			snprintf((char*)host_id_zmq.data(), clt_ID.length(), "%s", clt_ID.c_str());
			
			// then a new message id
			++msg_id;
			zmq::message_t message_id_zmq(sizeof(msg_id));
			memcpy(message_id_zmq.data(), &msg_id, sizeof(msg_id));
			
			// the name of the database
			std::string dbname="monitoringdb";
			zmq::message_t db_name_zmq(dbname.size()+1);
			snprintf((char*)db_name_zmq.data(), dbname.size()+1, "%s", dbname.c_str());
			
			// then the data  ( appending ::jsonb is optional )
			std::string messagedata = "INSERT INTO resources ( time, source, status ) "
			  "VALUES ( NOW(), '" + hostname + "', '" + resource_json + "'::jsonb )";
			if(verbosity>3) std::cout<<"sending write query '"<<messagedata<<"'"<<std::endl;
			
			zmq::message_t message_data_zmq(messagedata.size()+1);
			//memcpy(message_data_zmq.data(), messagedata.data(), messagedata.size());
			snprintf((char*)message_data_zmq.data(), messagedata.size()+1, "%s", messagedata.c_str());
			
			// send zmq message
			if(not (clt_pub_socket->send(host_id_zmq, ZMQ_SNDMORE))){
				std::cerr<<"Error sending write query pt 0"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else if(not (clt_pub_socket->send(message_id_zmq, ZMQ_SNDMORE))){
				std::cerr<<"Error sending write query pt 1"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else if(not (clt_pub_socket->send(db_name_zmq, ZMQ_SNDMORE))){
				std::cerr<<"Error sending write query pt 2!"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else if(not (clt_pub_socket->send(message_data_zmq))){
				std::cerr<<"Error sending write query pt 3!"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else {
				// else sent ok
				++write_queries_sent;
				// add to our queue of messages waiting for responses
				Message sent_msg(msg_id, messagedata, false);
				ack_queue.emplace(msg_id, sent_msg);
				last_write = boost::posix_time::microsec_clock::universal_time();
			}
			
		} // else not due to send another write query yet
		
	} // else no listeners on write port
else{
std::cout<<"no listener on write port"<<std::endl;
}
	
	// next read queries - check if we have a listener
	if(out_polls.at(1).revents & ZMQ_POLLOUT){
		
		// check if we're due to send a read query
		elapsed_time = boost::posix_time::microsec_clock::universal_time() - last_read;
		elapsed_time = read_period - elapsed_time;
		
		if( elapsed_time.is_negative() ){
			
			// time to send a read query
			
			// format zmq message
			
			// first the host id
			zmq::message_t host_id_zmq(clt_ID.length());
			snprintf((char*)host_id_zmq.data(), clt_ID.length(), "%s", clt_ID.c_str());
			
			// then a new message id
			++msg_id;
			zmq::message_t message_id_zmq(sizeof(msg_id));
			memcpy(message_id_zmq.data(), &msg_id, sizeof(msg_id));
			
			// then a database name
			std::string dbname="monitoringdb";
			zmq::message_t db_name_zmq(dbname.size()+1);
			snprintf((char*)db_name_zmq.data(), dbname.size()+1, "%s", dbname.c_str());
			
			// then the data
			//std::string messagedata = "SELECT * FROM resources WHERE time >= NOW() - INTERVAL '5 minutes'";
			std::string messagedata = "SELECT * FROM resources ORDER BY time LIMIT 3";
			zmq::message_t message_data_zmq(messagedata.size()+1);
			//memcpy(message_data_zmq.data(), messagedata.data(), messagedata.size());
			snprintf((char*)message_data_zmq.data(), messagedata.size()+1, "%s", messagedata.c_str());
			
			// send zmq message
			/* id is prepended automatically by receiving router.
			// this only happens for read queries, write queries are received by a subcriber
			// which does not do this
			if(not (clt_dlr_socket->send(host_id_zmq, ZMQ_SNDMORE))){
				std::cerr<<"Error sending read query pt 0"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else */
			 if(not (clt_dlr_socket->send(message_id_zmq, ZMQ_SNDMORE))){
				std::cerr<<"Error sending read query pt 1"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
		 	} else if(not (clt_dlr_socket->send(db_name_zmq, ZMQ_SNDMORE))){
				std::cerr<<"Error sending read query pt 2"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else if(not (clt_dlr_socket->send(message_data_zmq))){
				std::cerr<<"Error sending read query pt 3!"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else {
				// else sent ok
				++read_queries_sent;
				// add to our queue of messages waiting for responses
				Message sent_msg(msg_id, messagedata, true);
				ack_queue.emplace(msg_id, sent_msg);
				last_read = boost::posix_time::microsec_clock::universal_time();
			}
			
		} // else not due to send another read query yet.
		
	}
else {
std::cout<<"no listener on read port"<<std::endl;
}
	
	// finally any necessary re-sends
	// check if we have anything in the resend queue to resend
	//std::cout<<"we have "<<ack_queue.size()<<" messages awaiting acknowledgement"<<std::endl;
	
	for(std::pair<const int, Message>& amsg : ack_queue){
		
		Message& next_msg = amsg.second;
		unsigned int next_msg_id = amsg.first;
		
		// check if the time since sending is > re-send period
		elapsed_time = boost::posix_time::microsec_clock::universal_time() - next_msg.last_send_time;
		elapsed_time = resend_period - elapsed_time;
		
		if( elapsed_time.is_negative() && next_msg.retries < max_retries){
			
			// no response. Time to resend.
			
			// format zmq message
			
			// hostname
			zmq::message_t host_id_zmq(clt_ID.length());
			snprintf((char*)host_id_zmq.data(), clt_ID.length(), "%s", clt_ID.c_str());
			
			// use same message id
			zmq::message_t message_id_zmq(sizeof(next_msg_id));
			memcpy(message_id_zmq.data(), &next_msg_id, sizeof(next_msg_id));
			
			// then the data
			std::string messagedata = next_msg.query;
			zmq::message_t message_data_zmq(messagedata.size()+1);
			//memcpy(message_data_zmq.data(), messagedata.data(), messagedata.size());
			snprintf((char*)message_data_zmq.data(), messagedata.size()+1, "%s", messagedata.c_str());
			
			// check if it's a read or write query
			bool read_qry = next_msg.expect_response;
			
			// check if the appropriate socket has a listener
			int poll_index = (read_qry) ? 1 : 0;
			if(out_polls.at(poll_index).revents & ZMQ_POLLOUT){
				
				// get the appropriate socket to send on
				zmq::socket_t* sock = (read_qry) ? clt_dlr_socket : clt_pub_socket;
				
				// send zmq message
				if(!read_qry && not (sock->send(host_id_zmq, ZMQ_SNDMORE))){
					std::cerr<<"Error resending "<<((read_qry) ? "read" : "write")<<" query pt 0!"<<std::endl;
				} else if(not (sock->send(message_id_zmq, ZMQ_SNDMORE))){
					std::cerr<<"Error resending "<<((read_qry) ? "read" : "write")<<" query pt 1!"<<std::endl;
				} else if(not (sock->send(message_data_zmq))){
					std::cerr<<"Error resending "<<((read_qry) ? "read" : "write")<<" query pt 2!"<<std::endl;
				} else {
					// sent ok. increment the retry count and update the last send time.
					++next_msg.retries;
					next_msg.last_send_time = boost::posix_time::microsec_clock::universal_time();
					(read_qry) ? ++read_queries_resent : ++write_queries_resent;
					std::cout<<"no response for message "<<next_msg_id<<", resending"<<std::endl;
				}
				
			} else {
				// no listener. We'll increment the retries anyway,
				// basically because we don't want the clients to end up accumulating messages
				// while the server is down.
				++next_msg.retries;
				next_msg.last_send_time = boost::posix_time::microsec_clock::universal_time();
			}
			
		} else if(elapsed_time.is_negative()) {
			
			// ack timeout expired, but we've hit our max retries.
			
			// discard the message
			ack_queue.erase(next_msg_id);
			
			++messages_discarded;
			
		} // else not due for resending yet
		
	} // else nothing in the resend queue
	
	// intermittently print stats locally for debug
	// check if the time since last stats printout is > printout period
	elapsed_time = boost::posix_time::microsec_clock::universal_time() - last_printout;
	elapsed_time = print_stats_period - elapsed_time;
	
	if(elapsed_time.is_negative()){
		
		// time to print some stats
		std::cout<<"reads sent/resent: "<<read_queries_sent<<"/"<<read_queries_resent<<"\n"
		         <<"writes sent/resent: "<<write_queries_sent<<"/"<<write_queries_resent<<"\n"
		         <<"acks ignored: "<<acks_ignored<<"\n"
		         <<"total acks expected: "<<(read_queries_sent+write_queries_sent+acks_ignored)<<"\n"
		         <<"acks ok/err/other: "<<ok_acks_received<<"/"<<err_acks_received<<"/"<<other_acks_received<<"\n"
		         <<"acks for unrecognised message ids: "<<unknown_acks_received<<"\n"
		         <<"messages discarded due to lack of acknowledgement: "<<messages_discarded<<"\n"
		         <<std::flush;
		
		last_printout = boost::posix_time::microsec_clock::universal_time();
	}
	
	// test the logging port
	Log("Test log message "+std::to_string(execute_iterations),1);
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool DemoClient::GetResourceUsage(std::string& resource_json){
	// get the variables (e.g. cpu / mem / io stats)
	// this is easiest to do with system calls, so we'll put them into a file to access the results
	// TODO compstats should probably go into /tmp ...?
	std::string cmd;
	
	cmd = "printf \"%s%s\n\" \"Disk \" `df -h | grep /dev/sdb1 | awk '{ print $5}'|sed s:'\%'::` > ./compstats";
	// usage of /dev/sdb1 (source disk of /mnt/data) in %
	get_ok = system(cmd.c_str());
	
	// free cpu usage %
	cmd = "printf \"%s%s\n\" \"CPUidle \" `mpstat 1 1 | tail -n 1 | awk '{print $12}'` >> ./compstats";
	get_ok += system(cmd.c_str());
	
	// total physical memory kB
	cmd = "cat /proc/meminfo | grep MemTotal |sed s/://g  | sed s:kB::g >> ./compstats";
	get_ok += system(cmd.c_str());
	
	// free memory kB - https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=34e431b0ae398fc54ea69ff85ec700722c9da773
	cmd = "cat /proc/meminfo | grep MemAvailable |sed s/://g  | sed s:kB::g >> ./compstats";
	get_ok += system(cmd.c_str());
	
	// also works
	//cmd = "free -m | grep Mem | awk '{ print $7}'";  // 2 is total, 7 is available, `-m` says in MB
	// can also pipe straight into bc to calculate percentage. 'scale=2 for 1 sig fig...'
	//cmd = "free -m | grep Mem | awk '{print "scale=1;", $7, "/", $2, "*100"}' | bc"
	
	// uptime
	cmd = "cat /proc/uptime | awk '{print \"Uptime \" $1}' >> ./compstats";
	get_ok += system(cmd.c_str());
	
	if(get_ok != 0){
		// system call failed
		std::cerr<<"resource dump system call failed"<<std::endl;
		return false;
	}
	
	// ok, now read that file into a Store
	Store tmp;
	tmp.Initialise("./compstats");
	
	// we still need to process them a bit to make them nicer, so get the values
	double cpufreepercent;
	double ramtotalkB;
	double ramavailablekB;
	double uptimesecs;
	double diskusedpercent;
	get_ok  = tmp.Get("Disk",diskusedpercent);
	get_ok &= tmp.Get("MemAvailable", ramavailablekB);
	get_ok &= tmp.Get("MemTotal", ramtotalkB);
	get_ok &= tmp.Get("CPUidle", cpufreepercent);
	get_ok &= tmp.Get("Uptime", uptimesecs);
	if(not get_ok){
		std::cerr<<"Unable to get resources from compstats file?"<<std::endl;
		return false;
	}
	
	// calculate the values we really want
	double cpuusedpercent = 100. - cpufreepercent;
	double ramusedpercent = (ramtotalkB - ramavailablekB) / ramtotalkB;
	
	// and put them into a json to send to the database
	Store tmp2;
	tmp2.Set("cpu_usage_percent",cpuusedpercent);
	tmp2.Set("ram_usage_percent",ramusedpercent);
	tmp2.Set("disk_usage_percent",diskusedpercent);
	tmp2.Set("uptime_secs",uptimesecs);
	tmp2 >> resource_json;
	
	// print for debug
	if(verbosity>4){
		std::cout<<"cpu usage: "<<cpuusedpercent<<"\n"
		         <<"ram usage: "<<ramusedpercent<<"\n"
		         <<"disk usage: "<<diskusedpercent<<"\n"
		         <<"uptime: "<<uptimesecs<<"\n"
		         <<std::flush;
		std::cout<<"json string: '"<<resource_json<<"'"<<std::endl;
	}
	
	return true;
}

bool DemoClient::Finalise(){
	std::cout<<"Removing services"<<std::endl;
	utilities->RemoveService("psql_write");
	utilities->RemoveService("psql_read");
	
	std::cout<<"Deleting ServiceDiscovery"<<std::endl;
	delete service_discovery; service_discovery=nullptr;
	
	std::cout<<"Deleting Utilities class"<<std::endl;
	delete utilities; utilities=nullptr;
	
	std::cout<<"deleting sockets"<<std::endl;
	delete clt_pub_socket; clt_pub_socket=nullptr; 
	delete clt_dlr_socket; clt_dlr_socket=nullptr;
	delete log_pub_socket; log_pub_socket=nullptr;
	
	std::cout<<"deleting context"<<std::endl;
	delete context; context=nullptr;
	
	std::cout<<"Finalise done"<<std::endl;
}


// Helper Functions

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool DemoClient::Log(std::string logmessage, int severity){
	
	bool all_ok = true;
	
	// check if we're due to send a log message
	elapsed_time = boost::posix_time::microsec_clock::universal_time() - last_log;
	elapsed_time = log_period - elapsed_time;
	
	if( elapsed_time.is_negative() ){
	
std::cout<<"time to send a log message"<<std::endl;
		// check we had a listener ready
		if(out_polls.at(2).revents & ZMQ_POLLOUT){
std::cout<<"got a log listener"<<std::endl;
		
			// OK to send! Form the message
			// the log message is a 4 part message
			// 1. the identity of the sender (us)
			// 2. time the message was logged
			// 3. the severity
			// 4. the message to log
			
			// first the host id
			zmq::message_t host_id_zmq(clt_ID.length());
			snprintf((char*)host_id_zmq.data(), clt_ID.length(), "%s", clt_ID.c_str());
			
			// then a timestamp
			std::string timestamp;
			timestamp.resize(19);
			time_t rawtime = time(NULL);
			struct tm* timeinfo = localtime(&rawtime);
			strftime(const_cast<char*>(timestamp.c_str()),20,"%F %T",timeinfo);
			zmq::message_t message_timestamp_zmq(timestamp.size()+1);
			snprintf((char*)message_timestamp_zmq.data(), timestamp.size()+1, "%s", timestamp.c_str());
			
			// then the severity
			zmq::message_t message_severity_zmq(sizeof(severity));
			memcpy(message_severity_zmq.data(), &severity, sizeof(severity));
			
			// then the log message
			zmq::message_t message_data_zmq(logmessage.size()+1);
			snprintf((char*)message_data_zmq.data(), logmessage.size()+1, "%s", logmessage.c_str());
			
			// send zmq message
std::cout<<"Sending logmessage '"<<logmessage<<"'"<<std::endl;
			all_ok = false;
			if(not (log_pub_socket->send(host_id_zmq, ZMQ_SNDMORE))){
				std::cerr<<"Error sending log message pt 0"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else if(not (log_pub_socket->send(message_timestamp_zmq, ZMQ_SNDMORE))){
				std::cerr<<"Error sending log message pt 1"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else if(not (log_pub_socket->send(message_severity_zmq, ZMQ_SNDMORE))){
				std::cerr<<"Error sending log message pt 2"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else if(not (log_pub_socket->send(message_data_zmq))){
				std::cerr<<"Error sending log message pt 3"<<std::endl;
				printf("Error was: %s\n",errnoname(errno));
				perror("which maps to: ");
			} else {
				// else sent ok
				all_ok = true;
				++log_msgs_sent;
				last_log = boost::posix_time::microsec_clock::universal_time();
			}
			
		} // else no available listeners
	} // else not yet time to send a log message
	
	return all_ok;
}

