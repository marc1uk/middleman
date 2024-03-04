/* vim:set noexpandtab tabstop=4 wrap filetype=cpp */
#include "ReceiveSQL.h"
#include <exception>
#include <stdio.h>
#include <cstring>
#include <ctime>
#include <locale>    // toupper
#include <thread>
#include <chrono>

// TODO: invoking pg_promote requires either superuser privileges, or explicit granting
// of EXECUTE on the function pg_promote. We should grant this to the middleman,
// which otherwise runs as the toolanalysis user....

// TODO check validity of reinterpret_casts?

//                   ≫ ──── ≪•◦ ❈ ◦•≫ ──── ≪
//                        Main Program Parts
//                   ≫ ──── ≪•◦ ❈ ◦•≫ ──── ≪

bool ReceiveSQL::Initialise(const std::string& configfile){
	Store m_variables;
	Log("Reading config",3);
	m_variables.Initialise(configfile);
	std::string zmq_id = "1";
	m_variables.Get("zmq_identity",zmq_id);
	my_id = "middleman_" + zmq_id;
	
	// needs to be done first (or at least before InitZMQ)
	Log("Initialising Messaging Queues",3);
	get_ok = InitMessaging(m_variables);
	if(not get_ok) return false;
	
	Log("Initialising database",3);
	get_ok = InitPostgres(m_variables, "daq"); // FIXME extend for >1 database
	if(not get_ok) return false;
	
	Log("Initialising multicast sockets",3);
	get_ok = InitMulticast(m_variables);
	if(not get_ok) return false;
	
	Log("Initializing ZMQ sockets",3);
	get_ok = InitZMQ(m_variables);
	if(not get_ok) return false;
	
	Log("Initializing ServiceDiscovery",3);
	get_ok = InitServiceDiscovery(m_variables);
	if(not get_ok) return false;
	
	Log("Initializing SlowControls",3);
	get_ok = InitControls(m_variables);
	if(not get_ok) return false;
	
	return true;
}

bool ReceiveSQL::Execute(){
	Log("ReceiveSQL Executing...",5);
	
	// find new clients
	Log("Finding new clients",4);
	get_ok = FindNewClients_v2();
	
	// poll the input sockets for messages
	Log("Polling input sockets",4);
	try {
		get_ok = zmq::poll(in_polls.data(), in_polls.size(), inpoll_timeout);
	} catch (zmq::error_t& err){
		std::cerr<<"ReceiveSQL::Execute input poller caught "<<err.what()<<std::endl;
		get_ok = -1;
	}
	if(get_ok<0){
		Log("Warning! ReceiveSQL error polling input sockets; have they closed?",0);
	}
	
	// receive inputs
	if(am_master){
		Log("Getting Client Write Queries",4);
		get_ok = GetClientWriteQueries();
	}
	Log("Getting Client Read Queries",4);
	get_ok = GetClientReadQueries();
	if(am_master){
		Log("Getting Client Multicast Messages",4);
		get_ok = GetMulticastMessages();
	}
	Log("Getting Middleman Checkin",4);
	get_ok = GetMiddlemanCheckin();
	Log("Checking Master Status",4);
	get_ok = CheckMasterStatus();
	if(am_master){
		Log("Running Next Write Query",4);
		get_ok = RunNextWriteQuery();
	}
	Log("Running Next Read Query",4);
	get_ok = RunNextReadQuery();
	if(am_master){
		Log("Running Next Log Message",4);
		get_ok = RunNextLogMsg();
		Log("Running Next Monitoring Message",4);
		get_ok = RunNextMonitoringMsg();
	}
	
	// poll the output sockets for listeners
	Log("Polling output sockets",4);
	try {
		get_ok = zmq::poll(out_polls.data(), out_polls.size(), outpoll_timeout);
	} catch (zmq::error_t& err){
		std::cerr<<"ReceiveSQL::Execute output poller caught "<<err.what()<<std::endl;
		get_ok = -1;
	}
	if(get_ok<0){
		Log("Warning! ReceiveSQL error polling output sockets; have they closed?",0);
	}
	
	// send outputs
	Log("Sending Next Client Response",4);
	get_ok = SendNextReply();
	Log("Sending Next Log Message",4);
	get_ok = SendNextLogMsg();
	Log("Broadcasting Presence",4);
	get_ok = BroadcastPresence();
	
	// Maintenance
	Log("Trimming Write Queue",4);
	get_ok = TrimQueue("wrt_txn_queue");
	Log("Trimming Read Queue",4);
	get_ok = TrimQueue("rd_txn_queue");
	Log("Trimming Ack Queue",4);
	get_ok = TrimQueue("response_queue");
	Log("Trimming In Logging Deque",4);
	get_ok = TrimDequeue("in_log_queue");
	Log("Trimming Out Logging Deque",4);
	get_ok = TrimDequeue("out_log_queue");
	Log("Trimming In Monitoring Deque",4);
	get_ok = TrimDequeue("in_monitoring_queue");
	Log("Trimming Cache",4);
	get_ok = TrimCache();
	Log("Cleaning Up Old Cache Messages",4);
	get_ok = CleanupCache();
	
	// Monitoring and Logging
	Log("Tracking Stats",4);
	if(!stats_period.is_negative()) get_ok = TrackStats();
	
	// Check for any commands from remote control port
	Log("Checking Controls",4);
	get_ok = UpdateControls();
	
	Log("Loop Iteration Done",5);
	return true;
}

bool ReceiveSQL::Finalise(){
	
	Log("Closing middleman",3);
	
	// remove our services from those advertised?
	Log("Removing Discoverable Services",3);
	if(utilities) utilities->RemoveService("middleman");
	
	Log("Closing multicast socket",3);
	close(multicast_socket);
	
	Log("Deleting Utilities",3);
	if(utilities){ delete utilities; utilities=nullptr; }
	
	Log("Deleting ServiceDiscovery",3);
	if(service_discovery){ delete service_discovery; service_discovery=nullptr; }
	
	Log("Clearing known connections",3);
	clt_rtr_connections.clear();
	mm_rcv_connections.clear();
	clt_sub_connections.clear();
	
	// delete sockets
	Log("Deleting sockets",3);
	if(clt_sub_socket){ delete clt_sub_socket; clt_sub_socket=nullptr; }
	if(clt_rtr_socket){ delete clt_rtr_socket; clt_rtr_socket=nullptr; }
	if(mm_rcv_socket) { delete mm_rcv_socket;  mm_rcv_socket=nullptr;  }
	if(mm_snd_socket) { delete mm_snd_socket;  mm_snd_socket=nullptr;  }
	// delete zmq context
	
	// clear all message queues
	Log("Clearing message queues",3);
	wrt_txn_queue.clear();
	rd_txn_queue.clear();
	resp_queue.clear();
	cache.clear();
	in_log_queue.clear();
	out_log_queue.clear();
	in_monitoring_queue.clear();
	
	Log("Deleting context",3);
	if(context){ delete context; context=nullptr; }
	
	Log("Done, returning",3);
	return true;
}

//                   ≫ ──── ≪•◦ ❈ ◦•≫ ──── ≪
//                        Main Subroutines
//                   ≫ ──── ≪•◦ ❈ ◦•≫ ──── ≪

bool ReceiveSQL::InitPostgres(Store& m_variables, const std::string& dbname){
	
	// ##########################################################################
	// default initialize variables
	// ##########################################################################
	std::string dbhostname = "/tmp";     // '/tmp' = local unix socket
	std::string dbhostaddr = "";         // fallback if hostname is empty, an ip address
	int dbport = 5432;                   // database port
	std::string dbuser = "";             // database user to connect as. defaults to PGUSER env var if empty.
	std::string dbpasswd = "";           // database password. defaults to PGPASS or PGPASSFILE if not given.
	
	// on authentication: we may consider using 'ident', which will permit the
	// user to connect to the database as the postgres user with name matching
	// their OS username, and/or the database user mapped to their username
	// with the pg_ident.conf file in postgres database. in such a case dbuser and dbpasswd
	// should be left empty
	
	// ##########################################################################
	// # Update with user-specified values.
	// ##########################################################################
	// TODO make these config var keys specific for each db.
	m_variables.Get("hostname",dbhostname);
	m_variables.Get("hostaddr",dbhostaddr);
	m_variables.Get("port",dbport);
	m_variables.Get("user",dbuser);
	m_variables.Get("passwd",dbpasswd);
	
	// ##########################################################################
	// # Open connection
	// ##########################################################################
	
	// pass connection details to the postgres interface class
	Postgres& m_database = m_databases[dbname];
	m_database.Init(dbhostname,
	                dbhostaddr,
	                dbport,
	                dbuser,
	                dbpasswd,
	                dbname);
	
	// try to open a connection to ensure we can do, or else bail out now.
	if(!m_database.OpenConnection()){
		Log(Concat("Error! Failed to open connection to the ",dbname," database!"),0);
		return false;
	}
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::InitMulticast(Store& m_variables){
	
	/*              Multicast Setup              */
	/* ----------------------------------------- */
	
	int multicast_port = 55554;
	std::string multicast_address = "239.192.1.1"; // FIXME suitable default?
	
	// FIXME add to config file
	m_variables.Get("multicast_port",multicast_port);
	m_variables.Get("multicast_address",multicast_address);
	
	// set up multicast socket for sending logging & monitoring data
	multicast_socket = socket(AF_INET, SOCK_DGRAM, 0);
	if(multicast_socket<=0){
		Log(std::string{"Failed to open multicast socket with error "}+strerror(errno),v_error);
		return false;
	}
	
	// set linger options - do not linger, discard queued messages on socket close
	struct linger l;
	l.l_onoff  = 0;  // whether to linger
	l.l_linger = 0;  // seconds to linger for
	get_ok = setsockopt(multicast_socket, SOL_SOCKET, SO_LINGER, (char*) &l, sizeof(l));
	int a =1;
	setsockopt(multicast_socket, SOL_SOCKET, SO_REUSEADDR, &a, sizeof(a));
	if(get_ok!=0){
		Log(std::string{"Failed to set multicast socket linger with error "}+strerror(errno),v_error);
		return false;
	}
	
	// set the socket to non-blocking mode - seems like a good idea...? XXX
	get_ok = fcntl(multicast_socket, F_SETFL, O_NONBLOCK);
	if(get_ok!=0){
		Log(std::string{"Failed to set multicast socket to non-blocking with error "}+strerror(errno),v_error);
		//return false;
	}
	
	// format destination address from IP string
	bzero((char *)&multicast_addr, sizeof(multicast_addr)); // init to 0
	multicast_addr.sin_family = AF_INET;
	multicast_addr.sin_port = htons(multicast_port);
	// sending: which multicast group to send to
	get_ok = inet_aton(multicast_address.c_str(), &multicast_addr.sin_addr);
	if(get_ok==0){
		Log("Bad multicast address '"+multicast_address+"'",v_error);
		return false;
	}
	multicast_addrlen = sizeof(multicast_addr);
	
	/*
	// for two-way comms, we should bind to INADDR_ANY, not a specific multicast address.... maybe?
	struct sockaddr_in multicast_addr2;
	bzero((char *)&multicast_addr2, sizeof(multicast_addr2)); // init to 0
	multicast_addr2.sin_family = AF_INET;
	multicast_addr2.sin_port = htons(multicast_port);
	multicast_addr2.sin_addr.s_addr = htonl(INADDR_ANY);      << like this
	*/
	
	// to listen we need to bind to the socket
	get_ok = bind(multicast_socket, (struct sockaddr*)&multicast_addr, sizeof(multicast_addr));
	if(get_ok<0) {
		Log("Failed to bind to multicast listen socket",v_error);
		return false;
	}
	
	// and join a multicast group
	struct ip_mreq mreq;
	mreq.imr_interface.s_addr = htonl(INADDR_ANY);
	get_ok = inet_aton(multicast_address.c_str(), &mreq.imr_multiaddr);
	if(get_ok==0){
		Log("Bad multicast group '"+multicast_address+"'",v_error);
		return false;
	}
	get_ok = setsockopt(multicast_socket, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq));
	if(get_ok<0){
		Log("Failed to join multicast group",v_error);
		return false;
	}
	
	// we can poll with zmq ...
	in_polls.emplace_back(zmq::pollitem_t{NULL, multicast_socket, ZMQ_POLLIN, 0});
	out_polls.emplace_back(zmq::pollitem_t{NULL, multicast_socket, ZMQ_POLLOUT, 0});
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::InitZMQ(Store& m_variables){
	
	// ##########################################################################
	// # default initialize variables
	// ##########################################################################
	
	// the underlying zmq context can use multiple threads if we have really heavy traffic
	int context_io_threads = 1;
	
	// we have five zmq sockets:
	// 1. [SUB]    one for receiving published write queries from clients
	// 2. [DEALER] one for receiving dealt read queries from clients
	// 3. [ROUTER] one for sending back responses to queries we've run
	// 4. [PUB]    one for sending messages to the other middleman
	// 5. [SUB]    one for receiving messages from the other middleman
	// 6. [SUB]    one for receiving logging messages for the monitoring database, if we're the master.
	// 7. [PUB]    one for sending logging messages to the other middleman if we're not the master
	
	// the number and types of sockets are dictated by the messaging architecture
	// 1. we use SUB because clients send write requests to both middlemen, so they don't need to keep track
	// of who is the master. Both middlemen will receive all write requests, but only the master should
	// execute them - the standby discards them.
	// 2. we use a DEALER to receive read queries since they're round robined by clients, allowing load
	// distribution between the master and hot standby.
	// 3. we use ROUTER since we need to asynchronously send (or not, as required) acknowledgement replies
	// to a specified recipient.
	// 4/5. we use a pair of PUB/SUB sockets so that each middleman can broadcast its presence
	// without requiring a listener (should the other middleman go down).
	// 6/7. we use PUB/SUB so that clients can publish logging messages without worrying about who
	// is the master. They won't require a reply.
	
	// specify the ports everything talks on
	mm_snd_port =  55597;         // for sending middleman beacons
	// listeners connect to whatever remote port is picked up by ServiceDiscovery, so normally
	// the middleman doesn't need to know what ports to listen on, only those it sends on.
	// But we're now doing away with advertising all ports, and connecting to invisible endpoints.
	clt_rtr_port = 55555;         // for client routers sending reads
	clt_sub_port = 55556;         // for client pubbers sending writes.
	
	// socket timeouts, so nothing blocks indefinitely
	clt_sub_socket_timeout=500;
	int clt_rtr_socket_timeout=500; // used for both sends and receives
	int mm_rcv_socket_timeout=500;
	int mm_snd_socket_timeout=500;
	
	// poll timeouts - we can poll multiple sockets at once to consolidate our polling deadtime.
	// the poll operation can also be used to prevent the cpu railing.
	// we keep the in poll and out poll operations separate as the in poll will always be run,
	// whereas the send sockets may not be polled if we have nothing to send.
	// (i suppose we could combine them, and poll the output sockets regardless...)
	// units are milliseconds
	inpoll_timeout=500;
	outpoll_timeout=500;
	
	// ##########################################################################
	// # Update with user-specified values.
	// ##########################################################################
	// If not given, default value will be retained.
	m_variables.Get("context_io_threads",context_io_threads);
	m_variables.Get("mm_snd_port",mm_snd_port);
	m_variables.Get("clt_sub_socket_timeout",clt_sub_socket_timeout);
	m_variables.Get("clt_rtr_socket_timeout",clt_rtr_socket_timeout);
	m_variables.Get("mm_rcv_socket_timeout",mm_rcv_socket_timeout);
	m_variables.Get("mm_snd_socket_timeout",mm_snd_socket_timeout);
	m_variables.Get("inpoll_timeout",inpoll_timeout);
	m_variables.Get("outpoll_timeout",outpoll_timeout);
	m_variables.Get("clt_rtr_port", clt_rtr_port);
	m_variables.Get("clt_sub_port", clt_sub_port);
	
	// these must match
	mm_rcv_port = mm_snd_port;    // for other middlemen sending beacons
	
	// ##########################################################################
	// # Open connections
	// ##########################################################################
	
	// open a zmq context
	context = new zmq::context_t(context_io_threads);
	
	if(am_master){
		// socket to receive published write queries from clients
		// -------------------------------------------------------
		clt_sub_socket = new zmq::socket_t(*context, ZMQ_SUB);
		// this socket never sends, so a send timeout is irrelevant.
		clt_sub_socket->setsockopt(ZMQ_RCVTIMEO, clt_sub_socket_timeout);
		// don't linger too long, it looks like the program crashed.
		clt_sub_socket->setsockopt(ZMQ_LINGER, 10);
		clt_sub_socket->setsockopt(ZMQ_SUBSCRIBE,"",0);
		// we will connect this socket to clients with the utilities class
	}
	
	// socket to receive dealt read queries and send responses to clients
	// ------------------------------------------------------------------
	clt_rtr_socket = new zmq::socket_t(*context, ZMQ_ROUTER);
	clt_rtr_socket->setsockopt(ZMQ_SNDTIMEO, clt_rtr_socket_timeout);
	clt_rtr_socket->setsockopt(ZMQ_RCVTIMEO, clt_rtr_socket_timeout);
	// don't linger too long, it looks like the program crashed.
	clt_rtr_socket->setsockopt(ZMQ_LINGER, 10);
	// FIXME remove- for debug only:
	// make reply socket error out if the destination is unreachable
	// (normally it silently drops the message)
	clt_rtr_socket->setsockopt(ZMQ_ROUTER_MANDATORY, 1);
	// we'll connect this socket to clients with the utilities class
	
	// socket to listen for presence of the other middleman
	// ----------------------------------------------------
	mm_rcv_socket = new zmq::socket_t(*context, ZMQ_SUB);
	mm_rcv_socket->setsockopt(ZMQ_RCVTIMEO, mm_rcv_socket_timeout);
	// this socket never sends, so a send timeout is irrelevant.
	// don't linger too long, it looks like the program crashed.
	mm_rcv_socket->setsockopt(ZMQ_LINGER, 10);
	mm_rcv_socket->setsockopt(ZMQ_SUBSCRIBE,"",0);
	// we'll connect this socket to clients with the utilities class
	
	// socket to broadcast our presence to the other middleman
	// -------------------------------------------------------
	mm_snd_socket = new zmq::socket_t(*context, ZMQ_PUB);
	mm_snd_socket->setsockopt(ZMQ_SNDTIMEO, mm_snd_socket_timeout);
	// this socket never receives, so a recieve timeout is irrelevant.
	// don't linger too long, it looks like the program crashed.
	mm_snd_socket->setsockopt(ZMQ_LINGER, 10);
	// this one we're a publisher, so we do bind.
	mm_snd_socket->bind(std::string("tcp://*:")+std::to_string(mm_snd_port));
	
	// make items to poll the input and output sockets
	in_polls.emplace_back(zmq::pollitem_t{*clt_rtr_socket,0,ZMQ_POLLIN,0});
	out_polls.emplace_back(zmq::pollitem_t{*clt_rtr_socket,0,ZMQ_POLLOUT,0});
	in_polls.emplace_back(zmq::pollitem_t{*mm_rcv_socket,0,ZMQ_POLLIN,0});
	out_polls.emplace_back(zmq::pollitem_t{*mm_snd_socket,0,ZMQ_POLLOUT,0});
	
	// if we're the master we have an extra socket for receiving database writes.
	// put it at the end so we can add/remove them as we get promoted/demoted.
	if(am_master){
		in_polls.emplace_back(zmq::pollitem_t{*clt_sub_socket,0,ZMQ_POLLIN,0});
	}
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::InitServiceDiscovery(Store& m_variables){
	
	// Use a service discovery class to locate clients.
	
	// Read service discovery configs into a Store
	std::string service_discovery_config;
	m_variables.Get("service_discovery_config", service_discovery_config);
	Store service_discovery_configstore;
	service_discovery_configstore.Initialise(service_discovery_config);
	
	// we'll need to send broadcasts to advertise our presence to the other middleman
	bool send_broadcasts = true;
	// we also need to listen for other service providers - the clients and the other middleman
	bool rcv_broadcasts = true;
	
	// multicast address and port to listen for other services on.
	std::string broadcast_address = "239.192.1.1";
	int broadcast_port = 5000;
	service_discovery_configstore.Get("broadcast_address",broadcast_address);
	service_discovery_configstore.Get("broadcast_port",broadcast_port);
	
	// how frequently to broadcast - note that we send intermittent messages about our status
	// over the mm_snd_port anyway, so the ServiceDiscovery is only used for the initial discovery.
	// since we also use the mm_snd_sock and mm_rcv_sock for negotiation, we can't do without them,
	// so it's probably not worth trying to move this functionality into the ServiceDiscovery class.
	int broadcast_period_sec = 5;
	service_discovery_configstore.Get("broadcast_period",broadcast_period_sec);
	
	// whenever we broadcast any services the ServiceDiscovery class automatically advertises
	// the presence of a remote control port. It will attempt to check the status of that remote
	// control service on each broadcast by querying localhost:remote_control_port.
	// For now, unless we implement a listener to respond to those messages,
	// the zmq poll will timeout and the reported Status will just be "Status".
	// (the RemoteControl Service is normally implemented as part of ToolDAQ)
	// for the moment, this is N/A
	int remote_control_port = 24011;
	m_variables.Get("remote_control_port",remote_control_port);
	
	// The Utilities class has a helper function that will connect a given zmq socket
	// to all broadcasters with a given service name.
	// The name given in the ServiceDiscovery constructor is used for the RemoteControl service;
	// for this we use our unique id (my_id). We also need to provide a unique identifier
	// used by the ServiceDiscovery listener thread,
	// which maintains a map of services it's recently heard about, for which this is the key.
	boost::uuids::uuid client_id = boost::uuids::random_generator()();
	
	// The ServiceDiscovery class maintains a list of services it hears from, but it'll prune
	// services that it hasn't heard from after a while so that it doesn't retain stale ones.
	// How long before we prune, in seconds
	int kick_secs = 30;
	service_discovery_configstore.Get("kick_secs",kick_secs);
	
	// construct the ServiceDiscovery instance.
	service_discovery = new ServiceDiscovery(send_broadcasts, rcv_broadcasts, remote_control_port,
	                        broadcast_address, broadcast_port, context, client_id,
	                        my_id, broadcast_period_sec, kick_secs);
	// it'll handle discovery & broadcasting in a separate thread - we don't need to do anything else.
	
	// version that only listens, doesn't broadcast
	//service_discovery = new ServiceDiscovery(broadcast_address, broadcast_port, context, kick_secs);
	
	// Register Services
	// -----------------
	// to connect our listener ports to the clients discovered with the ServiceDiscovery class
	// we can use a Utilities class
	utilities = new Utilities(context);
	
	// this lets us connect our listener socket to all clients advertising a given service with:
	// Utilities::UpdateConnections({service_name}, {listener_socket}, {map_connections});
	// where map_connections is used to keep track of which nodes we're connected to.
	// This is called in FindNewClients at the end of Execute loop.
	// We can register services we wish to broadcast (so that others may connect to us) as follows:
	/*
	utilities->AddService("middleman", mm_snd_port);
	*/
	// for now we comment this out as ben prefers to assume such services are implicitly available
	// (hence FindNewClients->FindNewClientsv2)
	
	// note that it is not necessary to register the RemoteControl service,
	// this is automatically done by the ServiceDiscovery class.
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::InitControls(Store& m_variables){
	
	m_variables.Get("stopfile",stopfile);
	m_variables.Get("quitfile",quitfile);
	int remote_control_port = 24011;
	m_variables.Get("remote_control_port",remote_control_port);
	int remote_control_poll_period = 500;
	m_variables.Get("remote_control_poll_period", remote_control_poll_period);
	
	// the SlowControlCollection class runs a thread which communicates with RemoteControl services or webpages
	SC_vars.InitThreadedReceiver(context, remote_control_port, remote_control_poll_period, false);
	
	// internally SlowControlCollection retains a map of SlowControlElement objects.
	// Each SlowControlElement has a type, which may be one of:
	// { BUTTON, VARIABLE, OPTIONS, COMMAND, INFO }
	// The value of the SlowControlElement is held in a an ASCII Store, along with:
	// - min, max, and step for VARIABLES,
	// - an indexed list of possible value for OPTIONS or COMMANDS, (key: option num (arbitrary), value: option name)
	
	// add the set of controls we support
	SC_vars.Add("Restart",SlowControlElementType(BUTTON));
	SC_vars["Restart"]->SetValue(false);
	
	SC_vars.Add("Quit",SlowControlElementType(BUTTON));
	SC_vars["Quit"]->SetValue(false);
	
	SC_vars.Add("Status",SlowControlElementType(INFO));
	SC_vars["Status"]->SetValue("Initialising");
	
	// the SlowControlCollection thread listens for requests to get or set the registered controls,
	// and responds with or updates its internal control values. Note that (for now) it does not
	// take any action to effect the state of things outside it, so we will need to manually keep
	// the internal variable values up-to-date, and check for any changed internal values
	// in order to act on received commands
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::InitMessaging(Store& m_variables){
	
	// ##########################################################################
	// # default initialize variables
	// ##########################################################################
	
	// Master-Standby Messaging
	// ------------------------
	// time between master-slave check-ins
	int broadcast_period_ms = 1000;
	// how long we go without a message from the master before we promote ourselves
	int promote_timeout_ms = 10000;
	// how long before we start logging warnings that the other middleman is lagging.
	mm_warn_timeout = 7000;
	// are we the master?
	am_master = false;
	// should we promote ourselves if we can't find the master
	dont_promote = false;
	// should we warn about missing standby?
	warn_no_standby = false;
	
	// Client messaging
	// ----------------
	// how many times we try to send a response if zmq fails
	max_send_attempts = 3;
	// how many postgres queries/responses to queue before we start dropping them
	warn_limit = 700;
	// how many postgres queries/responses to queue before we emit warnings
	drop_limit = 1000;
	// how long to retain responses in case they get lost, so we can resend them if the client asks
	int cache_period_ms = 10000;
	// if we get a write query over the dealer port, which should be used for read-only transactions,
	// do we just error out, or, if we're the master, do we just run it anyway...?
	handle_unexpected_writes = false;
	// how often to write out monitoring stats
	int stats_period_ms = 60000;
	
	// logging
	// -------
	stdio_verbosity = 3;  // print errors, warnings and messages
	db_verbosity = 2;     // log to database only errors and warnings
	
	// ##########################################################################
	// # Update with user-specified values.
	// ##########################################################################
	m_variables.Get("broadcast_period_ms",broadcast_period_ms);
	m_variables.Get("promote_timeout_ms",promote_timeout_ms);
	m_variables.Get("mm_warn_timeout",mm_warn_timeout);
	m_variables.Get("am_master",am_master);
	m_variables.Get("dont_promote",dont_promote);
	m_variables.Get("warn_no_standby",warn_no_standby);
	m_variables.Get("max_send_attempts",max_send_attempts);
	m_variables.Get("warn_limit",warn_limit);
	m_variables.Get("drop_limit",drop_limit);
	m_variables.Get("cache_period_ms",cache_period_ms);
	m_variables.Get("handle_unexpected_writes",handle_unexpected_writes);
	m_variables.Get("stats_period_ms",stats_period_ms);
	m_variables.Get("stdio_verbosity",stdio_verbosity);
	m_variables.Get("db_verbosity",db_verbosity);
	
	// ##########################################################################
	// # Conversions
	// ##########################################################################
	// convert times to boost for easy handling
	broadcast_period = boost::posix_time::milliseconds(broadcast_period_ms);
	promote_timeout = boost::posix_time::milliseconds(promote_timeout_ms);
	cache_period = boost::posix_time::milliseconds(cache_period_ms);
	stats_period = boost::posix_time::milliseconds(stats_period_ms);
	
	boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
	last_mm_receipt = now;
	last_mm_send = now;
	std::time_t stdtime(0);
	last_stats_calc = boost::posix_time::time_from_string("2002-01-20 23:59:59.000"); // some arbitrary old time
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::FindNewClients(){
	
	int new_connections=0;
	int old_connections=0;
	
	// update any connections
	old_connections=clt_rtr_connections.size();
	utilities->UpdateConnections("psql_read", clt_rtr_socket, clt_rtr_connections);
	new_connections+=clt_rtr_connections.size()-old_connections;
	old_connections=mm_rcv_connections.size();
	utilities->UpdateConnections("middleman", mm_rcv_socket, mm_rcv_connections);
	new_connections+=mm_rcv_connections.size()-old_connections;
	
	// additional listening for master on write query and logging ports
	if(am_master){
		old_connections=clt_sub_connections.size();
		utilities->UpdateConnections("psql_write", clt_sub_socket, clt_sub_connections);
		new_connections+=clt_sub_connections.size()-old_connections;
	}
	
	if(new_connections>0){
		Log("Made "+std::to_string(new_connections)+" new connections!",3);
	} else {
		Log("No new clients found",5);
	}
	
	/* needs fixing to uncomment
	std::cout<<"We have: "<<connections.size()<<" connected clients"<<std::endl;
	std::cout<<"Connections are: "<<std::endl;
	for(auto&& athing : connections){
		std::string service;
		athing.second->Get("msg_value",service);
		std::cout<<service<<" connected on "<<athing.first<<std::endl;
	}
	*/
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::FindNewClients_v2(){
	
	int clt_rtr_conns = clt_rtr_connections.size();
	int clt_sub_conns = clt_sub_connections.size();
	int mm_conns = mm_rcv_connections.size();
	
	int new_connections = utilities->ConnectToEndpoints(clt_rtr_socket, clt_rtr_connections, clt_rtr_port, clt_sub_socket, clt_sub_connections, clt_sub_port, mm_rcv_socket, mm_rcv_connections, mm_rcv_port);
	
	if(new_connections>0){
		Log("Made "+std::to_string(new_connections)+" new connections!",3);
		Log("Made "+std::to_string(clt_rtr_connections.size()-clt_rtr_conns)
		   +" new read/reply socket connections",v_debug);
		Log("Made "+std::to_string(clt_sub_connections.size()-clt_sub_conns)
		   +" new write socket connections",v_debug);
		Log("Made "+std::to_string(mm_rcv_connections.size()-mm_conns)
		   +" new middleman socket connections",v_debug);
	} else {
		Log("No new clients found",5);
	}
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::GetClientWriteQueries(){
	
	// see if we had any write requests from clients
	if(in_polls.at(3).revents & ZMQ_POLLIN){
		Log(">>> got a write query from client",4);
		
		++write_queries_recvd;
		// we did. receive next message.
		std::vector<zmq::message_t> outputs;
		
		get_ok = Receive(clt_sub_socket, outputs);
		
		if(not get_ok){
			Log(Concat("error receiving part ",outputs.size()+1," of Write query from client"),1);
			++write_query_recv_fails;
			return false;
		}
		
		// received message format should be a 4-part message
		
		
		// expected parts are:
		// 0. publisher topic
		// 1. ZMQ_IDENTITY of the sender client
		// 2. a unique ID used by the sender to match acknowledgements to sent messages
		// 3. a JSON string encapsulating the message
		
		//* the first zmq part sent by a pub socket is the pub 'topic'
		// this is matched by zmq sub sockets to the ZMQ_SUBSCRIBE pattern,
		// and any not matching are filtered out by zmq behind the scenes.
		// Use an empty subscribe option to receive all messages.
		// for non-empty subscribe options, the topic must match that prefix
		// e.g. subscribing to 'LOGGING' will receive all messages sent on
		// topics 'LOGGING' and 'LOGGING_DEBUG' etc. for example
		// multiple ZMQ_SUBSCRIBE calls can be stacked.
		// Note the matched topic is still returned as a message part, so you can
		// identify which topic the incoming message came from if you do.
		
		// *some users of the middleman don't use ZMQ_SUBSCRIBE, so don't send a topic part.
		// XXX such users should set o=1 here. (FIXME make config variable?)
		int o=0;
		
		if(outputs.size()!=4-o){
			Log(Concat("unexpected ",outputs.size()," part message in Write query from client"),1);
			//Log(Concat("client: '",client_str,"', msg_id: ",msg_int),4);
			//FIXME probably useful if we have these parts to retrieve them and identify the culprit
			
			++write_query_recv_fails;
			return false;
		}
		
		std::string topic="-"; std::string client_str="-"; std::string message="-";
		uint32_t msg_int=-1;
		
		if(o==0){
			topic.resize(outputs.at(0).size(),'\0');
			memcpy((void*)topic.data(),outputs.at(0).data(),outputs.at(0).size());
		}
		
		client_str.resize(outputs.at(1-o).size(),'\0');
		memcpy((void*)client_str.data(),outputs.at(1-o).data(),outputs.at(1-o).size());
		
//// FIXME FIXME FIXME
//std::cout<<"client ID is length "<<outputs.at(1-o).size()<<std::endl;
//for(int i=0; i<outputs.at(1-o).size(); ++i){
//	char* chars = (char*)outputs.at(1-o).data();
//	printf("next char: %x\n",chars[i]);
//}
//std::cout<<"client_str is length "<<client_str.length()<<std::endl;
//for(int i=0; i<client_str.size(); ++i){
//	char* chars = (char*)client_str.data();
//	printf("next char: %x\n",chars[i]);
//}
//std::cout<<"so client_str is "<<client_str<<std::endl;
//printf("client_str: %s\n",client_str.c_str());
//std::cout<<"compared to expected: "<<(client_str=="newdaq01")<<std::endl;
//client_str=client_str.substr(0,client_str.find('\0'));
//std::cout<<"after trimming to null: '"<<client_str<<"' == "<<client_str.compare("daq01")<<std::endl;
		
		msg_int = *reinterpret_cast<uint32_t*>(outputs.at(2-o).data());
		
		message.resize(outputs.at(3-o).size(),'\0');
		memcpy((void*)message.data(),outputs.at(3-o).data(),outputs.at(3-o).size());
		
		// to track messages already handled, we form a key from the client ID and message ID,
		// and will use this to track message processing
		
		std::pair<std::string, uint32_t> key{client_str,msg_int};
		
		// check if we've received this query before
		// 1. we may have this query queued, but haven't run it yet - ignore
		// 2. we may have run the query, but not yet sent the response - ignore
		// 3. we may have run the query and sent the response, but it got lost in the mail -
		//    re-add the response to the to-send queue.
		// otherwise add to our query queue.
		Log(Concat("RECEIVED WRITE QUERY FROM CLIENT '",client_str,"' with msg id ",msg_int),3);
		
		if(cache.count(key)){
			Log("We know this query...",10);
			// we've already run and sent the response to this query, resend it.
			resp_queue.emplace(key,cache.at(key));
			cache.erase(key);
			
		} else if(wrt_txn_queue.count(key)==0 && resp_queue.count(key)==0){
			Log("New query, adding to write queue",10);
			// we don't have it waiting either in to-run or to-respond queues.
			
			// convert the received JSON into an appropriate SQL query and database
			std::string query, database;
			get_ok = WriteMessageToQuery(topic, message, database, query);
			if(!get_ok){
				Log("Error parsing "+topic+" write query: '"+message+"'",v_error);
				++write_queries_failed;
				return false;
			}
			
			// construct a Query object to encapsulate the query and enqueue it.
			Query qry{outputs.at(1-o),outputs.at(2-o), database, query};
			
			Log(Concat("QUERY WAS: '",qry.query,"'"),20);
			wrt_txn_queue.emplace(key, qry);
			
		} // else we've already got it queued, ignore it.
		
	}// else no messages from clients
/*
else {
std::cout<<"no write queries at input port"<<std::endl;
}
*/
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::WriteMessageToQuery(const std::string& topic, const std::string& message, std::string& db_out, std::string& sql_out){
	Log(Concat("Forming SQL for write query with topic: '",topic,"', message: '",message,"'"),4);
	
	// write queries received on the pub port are JSON messages that we need to convert to SQL.
	BStore tmp;
	get_ok = parser.Parse(message, tmp);
	if(!get_ok){
		Log("WriteMessageToQuery error parsing message json '"+message+"'",v_error);
		return false;
	}
	
	// the JSON fields depend on the kind of data; pub sockets include a 'topic'
	// which we use to identify what kind of data this is.
	if(topic=="CONFIG"){
		
		db_out = "daq"; // FIXME db
		Postgres& a_database = m_databases.at(db_out);
		
		// make a new device config entry
		//time_t timestamp{0}; // seems to end up with corrupt data even though BStore.Get returns OK
		uint32_t timestamp{0};
		std::string device;
		std::string author;
		std::string description;
		std::string data;
		get_ok = tmp.Get("time",timestamp);  // may not be present, in which case use 0 -> i.e. now()
		get_ok  = tmp.Get("device",device);
		get_ok &= tmp.Get("author",author);
		get_ok &= tmp.Get("description",description);
		get_ok &= tmp.Get("data",data);
		if(!get_ok){
			Log("WriteMessageToQuery: missing fields in message '"+message+"'",v_error);
			return false;
		}
		
		// SQL sanitization
		get_ok  = a_database.pqxx_quote(device, device);
		get_ok &= a_database.pqxx_quote(author, author);
		get_ok &= a_database.pqxx_quote(description, description);
		get_ok &= a_database.pqxx_quote(data, data);
		if(!get_ok){
			Log("WriteMessageToQuery: error quoting fields in message '"+message+"'",v_error);
			return false;
		}
		
		// times are received in unix seconds since epoch, or 0 for 'now()'.
		// build an ISO 8601 timestamp ("2015-10-02 11:16:34+0100")
		// (the trailing "+0100" is number of [hours][mins] in local timezone relative to UTC)
		std::string timestring;
		if(timestamp==0){
			timestring="now()";
		} else {
			std::cout<<"converting time "<<timestamp<<" to timestring"<<std::endl;
			timestring.resize(22, '\0');
			struct tm* timeptr = gmtime((time_t*)&timestamp);
			std::cout<<"timeptr is "<<timeptr<<std::endl;
			if(timeptr==0){
				Log("gmtime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				return false; // we could fall back to now(), but we leave that decision to the user.
			}
			get_ok = strftime(timestring.data(), timestring.length(), "%F %T%Z", timeptr);
			if(get_ok==0){
				Log("strftime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				return false; // we could fall back to now(), but we leave that decision to the user.
			}
		}
		
		sql_out = "INSERT INTO device_config (time, device, version, author, description, data) VALUES ( '"
		        + timestring  + "',"
		        + device      + ","
		        + "(select COALESCE(MAX(version)+1,0) FROM device_config WHERE device="+device+"),"
		        + author      + ","
		        + description + ","
		        + data        + ") returning version;";
		
		Log(Concat("Resulting SQL: '",sql_out,"', database: '",db_out,"'"),4);
		
		return true;
	}
	
	else if(topic=="CALIBRATION"){
		
		db_out = "daq"; // FIXME db
		Postgres& a_database = m_databases.at(db_out);
		
		// insert new calibration data
		//time_t timestamp{0};
		uint32_t timestamp{0};
		std::string device;
		std::string description;
		std::string data;
		tmp.Get("time",timestamp);  // may not be present, in which case use 0 -> i.e. now()
		get_ok  = tmp.Get("device",device);
		get_ok &= tmp.Get("description",description);
		get_ok &= tmp.Get("data",data);
		if(!get_ok){
			Log("WriteMessageToQuery: missing fields in message '"+message+"'",v_error);
			return false;
		}
		
		// SQL sanitization
		get_ok  = a_database.pqxx_quote(device, device);
		get_ok &= a_database.pqxx_quote(description, description);
		get_ok &= a_database.pqxx_quote(data, data);
		if(!get_ok){
			Log("WriteMessageToQuery: error quoting fields in message '"+message+"'",v_error);
			return false;
		}
		
		// times are received in unix seconds since epoch, or 0 for 'now()'.
		// build an ISO 8601 timestamp ("2015-10-02 11:16:34+0100")
		// (the trailing "+0100" is number of [hours][mins] in local timezone relative to UTC)
		std::string timestring;
		if(timestamp==0){
			timestring="now()";
		} else {
			timestring.resize(22, '\0');
			struct tm* timeptr = gmtime((time_t*)&timestamp);
			if(timeptr==0){
				Log("gmtime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				return false; // we could fall back to now(), but we leave that decision to the user.
			}
			get_ok = strftime(timestring.data(), timestring.length(), "%F %T%Z", timeptr);
			if(get_ok==0){
				Log("strftime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				return false; // we could fall back to now(), but we leave that decision to the user.
			}
		}
		
		sql_out = "INSERT INTO calibration (time, device, version, description, data) VALUES ( '"
		        + timestring   + "',"
		        + device       + ","
		        + "(select COALESCE(MAX(version)+1,0) FROM calibration WHERE device="+device+"),"
		        + description  + ","
		        + data         + ") returning version;";
		
		Log(Concat("Resulting SQL: '",sql_out,"', database: '",db_out,"'"),4);
		
		return true;
		
	} else if(topic=="ALARM"){
		
		db_out = "daq"; // FIXME db
		Postgres& a_database = m_databases.at(db_out);
		
		// record a new alarm
		//time_t timestamp{0};
		uint32_t timestamp{0};
		std::string device;
		uint32_t level;
		std::string msg;
		tmp.Get("time",timestamp);  // may not be present, in which case use 0 -> i.e. now()
		get_ok  = tmp.Get("device",device);
		get_ok &= tmp.Get("level",level);
		get_ok &= tmp.Get("message",msg);
		if(!get_ok){
			Log("WriteMessageToQuery: missing fields in message '"+message+"'",v_error);
			return false;
		}
		
		// SQL sanitization
		get_ok  = a_database.pqxx_quote(device, device);
		get_ok &= a_database.pqxx_quote(msg, msg);
		if(!get_ok){
			Log("WriteMessageToQuery: error quoting fields in message '"+message+"'",v_error);
			return false;
		}
		
		// times are received in unix seconds since epoch, or 0 for 'now()'.
		// build an ISO 8601 timestamp ("2015-10-02 11:16:34+0100")
		// (the trailing "+0100" is number of [hours][mins] in local timezone relative to UTC)
		std::string timestring;
		if(timestamp==0){
			timestring="now()";
		} else {
			timestring.resize(22, '\0');
			struct tm* timeptr = gmtime((time_t*)&timestamp);
			if(timeptr==0){
				Log("gmtime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				return false; // we could fall back to now(), but we leave that decision to the user.
			}
			get_ok = strftime(timestring.data(), timestring.length(), "%F %T%Z", timeptr);
			if(get_ok==0){
				Log("strftime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				return false; // we could fall back to now(), but we leave that decision to the user.
			}
		}
		
		sql_out = "INSERT INTO alarms (time, device, level, alarm) VALUES ( '"
		        + timestring            + "',"
		        + device                + ","
		        + std::to_string(level) + ","
		        + msg                   + ");";
		
		Log(Concat("Resulting SQL: '",sql_out,"', database: '",db_out,"'"),4);
		
		return true;
		
	}
	
	// if not caught by now:
	Log("Error: unrecognised pub message topic: '"+topic+"'",v_error);
	return false;
	
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::GetClientReadQueries(){
	
	// check if we had any read transactions dealt to us
	if(in_polls.at(1).revents & ZMQ_POLLIN){
		Log(">>> got a read query from client",4);
		
		++read_queries_recvd;
		// We do. receive the next query
		std::vector<zmq::message_t> outputs;
		
		get_ok = Receive(clt_rtr_socket, outputs);
		
		if(not get_ok){
			Log(Concat("error receiving part ",outputs.size()+1," of Read query from client"),1);
			++read_query_recv_fails;
			return false;
		}
		
		// received message format should be a 4-part message
		if(outputs.size()!=4){
			Log(Concat("unexpected ",outputs.size()," part message in Read query from client"),1);
			// FIXME probably useful if we have these parts to retrieve them and identify the culprit
			// FIXME send a reply to client if we can get enough info
			++read_query_recv_fails;
			return false;
		}
		
		// The received message format should be the same format as for Write queries.
		// 0. client ID
		// 1. topic*
		// 2. message ID
		// 3. JSON message
		// again the two IDs form a key used to track messages already handled.
		
		//* although for now we're using a dealer socket, a topic part is included
		// to distinguish what kind of message this is
		
		std::string topic="-"; std::string client_str="-"; uint32_t msg_int=-1; std::string msg_string="-";
		
		client_str.resize(outputs.at(0).size(),'\0');
		memcpy((void*)client_str.data(),outputs.at(0).data(),outputs.at(0).size());
		
		topic.resize(outputs.at(1).size(),'\0');
		memcpy((void*)topic.data(),outputs.at(1).data(),outputs.at(1).size());
		
		msg_int = *reinterpret_cast<uint32_t*>(outputs.at(2).data());
		
		msg_string.resize(outputs.at(3).size(),'\0');
		memcpy((void*)msg_string.data(),outputs.at(3).data(),outputs.at(3).size());
		
		std::pair<std::string, uint32_t> key{client_str,msg_int};
		
		// check if we already know this query.
		if(cache.count(key)){
			// we've already run and sent the response to this query, resend it.
			std::cout<<"we know this query: re-sending cached reply of:"<<std::endl;
			//cache.at(key).Print();
			
			resp_queue.emplace(key,cache.at(key));
			cache.erase(key);
			
		} else if(rd_txn_queue.count(key)==0 && resp_queue.count(key)==0){
			Log("RECEIVED READ QUERY FROM CLIENT '"+client_str+"' with message id: "+std::to_string(msg_int),3);
			
			// convert the received JSON into an appropriate SQL and database
			std::string query, database;
			get_ok = ReadMessageToQuery(topic, msg_string, database, query);
			if(!get_ok){
				Log("Error parsing read query: '"+msg_string+"'",v_error);
				++read_queries_failed;
				return false;
			}
			
			// construct a Query object to encapsulate the query and enqueue it.
			Query qry{outputs.at(0),outputs.at(2), database, query};
			rd_txn_queue.emplace(key, qry);
			
		} // else we've already got this message queued, ignore it.
		
	} // else no read queries this time
/*
else {
std::cout<<"no read queries at input port"<<std::endl;
}
*/
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::ReadMessageToQuery(const std::string& topic, const std::string& message, std::string& db_out, std::string& sql_out){
	Log(Concat("Forming SQL for read query with topic: '",topic,"', message: '",message,"'"),4);
	
	// write queries received on the pub port are JSON messages that we need to convert to SQL.
	BStore tmp;
	get_ok = parser.Parse(message, tmp);
	if(!get_ok){
		Log("ReadMessageToQuery error parsing message json '"+message+"'",v_error);
		return false;
	}
	
	// the JSON fields depend on the kind of data.
	// Use the topic to identify what kind of data this is.
	if(topic=="QUERY"){
		
		// this one's easy, the user has already given us a database and SQL statement
		get_ok  = tmp.Get("database", db_out);
		get_ok &= tmp.Get("query",sql_out);
		if(!get_ok){
			Log("ReadMessageToQuery missing fields in message '"+message+"'",v_error);
			return false;
		}
		return true;
		
	} else if(topic=="CONFIG"){
		
		db_out = "daq";
		Postgres& a_database = m_databases.at(db_out);
		
		// get a new device config entry
		std::string device;
		int32_t version;
		get_ok  = tmp.Get("device",device);
		get_ok &= tmp.Get("version",version);
		if(!get_ok){
			return false;
			Log("ReadMessageToQuery missing fields in message '"+message+"'",v_error);
		}
		
		// SQL sanitization
		get_ok  = a_database.pqxx_quote(device, device);
		if(!get_ok){
			Log("WriteMessageToQuery: error quoting fields in message '"+message+"'",v_error);
			return false;
		}
		
		// if user requests version <0, give latest
		std::string versionstring;
		if(version<0){
			versionstring = "(SELECT MAX(version) FROM device_config WHERE device="+device+")";
		} else {
			versionstring = std::to_string(version);
		}
		
		sql_out = "SELECT data FROM device_config WHERE device="
		        + device + " AND version="
		        + versionstring+";";
		
		Log(Concat("Resulting SQL: '",sql_out,"', database: '",db_out,"'"),4);
		
		return true;
		
	} else if(topic=="CALIBRATION"){
		
		db_out = "daq";
		Postgres& a_database = m_databases.at(db_out);
		
		// get a calibration data entry
		std::string device;
		int32_t version;
		get_ok  = tmp.Get("device",device);
		get_ok &= tmp.Get("version",version);
		if(!get_ok){
			Log("ReadMessageToQuery missing fields in message '"+message+"'",v_error);
			return false;
		}
		
		// SQL sanitization
		get_ok  = a_database.pqxx_quote(device, device);
		if(!get_ok){
			Log("WriteMessageToQuery: error quoting fields in message '"+message+"'",v_error);
			return false;
		}
		
		// if user requests version <0, give latest
		std::string versionstring;
		if(version<0){
			versionstring = "(SELECT MAX(version) FROM device_config WHERE device="+device+")";
		} else {
			versionstring = std::to_string(version);
		}
		
		sql_out = "SELECT data FROM calibration WHERE device="
		        + device + " AND version="
		        + versionstring+";";
		
		Log(Concat("Resulting SQL: '",sql_out,"', database: '",db_out,"'"),4);
		
		return true;
		
	}
	
	// if not caught by now:
	Log("Error: unrecognised read query type: '"+message+"'",v_error);
	return false;
	
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»


bool ReceiveSQL::GetMulticastMessages(){
	
	// check for incoming message
	
	// see if we had any multicast messages
	if(in_polls.at(0).revents & ZMQ_POLLIN){
		Log(">>> got a multicast message from client",4);
		++log_msgs_recvd;
		
		// read the messge
		char message[512];
		int cnt = recvfrom(multicast_socket, message, sizeof(message), 0, (struct sockaddr*)&multicast_addr, &multicast_addrlen);
		if(cnt <= 0){
			Log(std::string{"Failed to receive on multicast socket with error '"}+strerror(errno)+"'",v_error);
			++log_msg_recv_fails;
			return false;
		}
		
		Log("Received multicast message from "+std::string{inet_ntoa(multicast_addr.sin_addr)}
		   +": '"+std::string{message}+"'",5);
		
		std::string database;
		std::string query;
		std::string topic;
		get_ok = MulticastMessageToQuery(message, topic, database, query);
		
		if(!get_ok){
			++log_msg_recv_fails;  // XXX doesn't separate monitoring/logging...
			return false;
		}
		
		// FIXME for now both logging and monitoring go to daq database
		if(topic=="logging"){
			in_log_queue.emplace_back(query);
			Log("Put client log message in queue: '"+query+"'",5);
			
		} else if(topic=="monitoring"){
			in_monitoring_queue.emplace_back(query);
			Log("Put client monitoring msg in queue: '"+query+"'",5);
			
		} else {
			// could not determine multicast type
			Log(std::string{"Unable to parse multicast message '"}+message+"'",v_error);
			++log_msg_recv_fails;
			return false;
			
		}
		
	} /*else { std::cout<<"no multicast messages"<<std::endl; }*/
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::MulticastMessageToQuery(const std::string& message, std::string& topic_out, std::string& db_out, std::string& sql_out){
	Log(Concat("Forming SQL for logging message: '",message,"'"),4);
	
	// write queries received on the pub port are JSON messages that we need to convert to SQL.
	BStore tmp;
	get_ok = parser.Parse(message, tmp);
	if(!get_ok){
		Log("MulticastMessageToQuery error parsing message json '"+message+"'",v_error);
		return false;
	}
	
	// the JSON fields depend on the kind of data. Unlike writes we have no topic here,
	// so we just need to use the JSON keys to identify what kind of data this is.
	if(tmp.Has("message")){
		
		topic_out = "logging";
		db_out = "daq";
		Postgres& a_database = m_databases.at(db_out);
		
		// logging message
		std::string device;
		//time_t timestamp{0};
		uint32_t timestamp{0};
		uint32_t severity;
		std::string msg;
		get_ok = tmp.Get("time",timestamp); // optional
		get_ok = tmp.Get("device",device);
		get_ok &= tmp.Get("severity",severity);
		get_ok &= tmp.Get("message",msg);
		if(!get_ok){
			Log("MulticastMessageToQuery: missing fields in message '"+message+"'",v_error);
			return false;
		}
		
		// SQL sanitization
		get_ok  = a_database.pqxx_quote(device, device);
		get_ok &= a_database.pqxx_quote(msg, msg);
		if(!get_ok){
			Log("MulticastMessageToQuery: error quoting fields in message '"+message+"'",v_error);
			return false;
		}
		
		// times are received in unix seconds since epoch, or 0 for 'now()'.
		// build an ISO 8601 timestamp ("2015-10-02 11:16:34+0100")
		// (the trailing "+0100" is number of [hours][mins] in local timezone relative to UTC)
		std::string timestring;
		if(timestamp==0){
			timestring="now()";
		} else {
			timestring.resize(22, '\0');
			struct tm* timeptr = gmtime((time_t*)&timestamp);
			if(timeptr==0){
				Log("gmtime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				return false; // we could fall back to now(), but we leave that decision to the user.
			}
			get_ok = strftime(timestring.data(), timestring.length(), "%F %T%Z", timeptr);
			if(get_ok==0){
				Log("strftime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				timestring="now()";
				// XXX perhaps for mutlicast messages as errors are not propagated back to sender,
				// it's better to fallback to the assumption of now()?
			}
		}
		
		// form into a suitable SQL query
		sql_out = "INSERT INTO logging ( time, device, severity, message ) VALUES ( '"
		        + timestring               + "',"
		        + device                   + ","
		        + std::to_string(severity) + ","
		        + msg                      + ");";
		
		Log(Concat("Resulting SQL: '",sql_out,"', database: '",db_out,"', topic: ",topic_out),4);
		
		return true;
		
	} else if(tmp.Has("data")){
		
		db_out = "daq";
		Postgres& a_database = m_databases.at(db_out);
		
		// monitoring data
		std::string device;
		//time_t timestamp{0};
		uint32_t timestamp{0};
		std::string data;
		tmp.Get("time",timestamp); // optional
		get_ok = tmp.Get("device",device);
		get_ok &= tmp.Get("data",data);
		if(!get_ok){
			Log("MulticastMessageToQuery: missing fields in message '"+message+"'",v_error);
			return false;
		}
		
		// SQL sanitization
		get_ok  = a_database.pqxx_quote(device, device);
		get_ok &= a_database.pqxx_quote(data, data);
		if(!get_ok){
			Log("MulticastMessageToQuery: error quoting fields in message '"+message+"'",v_error);
			return false;
		}
		
		std::string timestring;
		if(timestamp==0){
			timestring="now()";
		} else {
			timestring.resize(22, '\0');
			struct tm* timeptr = gmtime((time_t*)&timestamp);
			if(timeptr==0){
				Log("gmtime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				return false; // we could fall back to now(), but we leave that decision to the user.
			}
			get_ok = strftime(timestring.data(), timestring.length(), "%F %T%Z", timeptr);
			if(get_ok==0){
				Log("strftime error converting unix time '"+std::to_string(timestamp)+"' to timestamp",v_error);
				timestring="now()";   // XXX
			}
		}
		
		// form into a suitable SQL query
		sql_out = "INSERT INTO monitoring ( time, device, data ) VALUES ( '"
		        + timestring + "',"
		        + device     + ","
		        + data       + ");";
		
		Log(Concat("Resulting SQL: '",sql_out,"', database: '",db_out,"', topic: ",topic_out),4);
		
		return true;
		
	}
	
	// if not caught by now:
	Log("Error: unrecognised multicast query type: '"+message+"'",v_error);
	return false;
	
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::GetMiddlemanCheckin(){
	
	// see if we had a presence broadcast from the other middleman
	// as well as checking the master is still up, we also check whether both middlemen
	// are in the same role, and if so initiate negotiatiation to promote/demote as necessary.
	bool need_to_negotiate=false;
	
	// if negotiations are started by the other middleman, we will propagate
	// the received information to the Negotiation function
	std::string their_header="";
	std::string their_timestamp="";
	
	// keep reading from the SUB socket until there are no more waiting messages.
	// it's important we don't take any action until we've read out everything,
	// to ensure we don't start negotiation based on old, stale requests.
	while(in_polls.at(2).revents & ZMQ_POLLIN){
		
		// We do. Receive it.
		std::vector<zmq::message_t> outputs;
		get_ok = Receive(mm_rcv_socket, outputs);
		if(not get_ok){
			Log(Concat("error receiving message part ",outputs.size()+1," of message from middleman"),0);
			++mm_broadcast_recv_fails;  // FIXME this includes negotiation requests and our own broadcasts
			return false;
		}
		
		// get the identity of the sender
		if(outputs.size()>0){
			std::string sender_id(outputs.at(0).size(),'\0');
			memcpy((void*)sender_id.data(),outputs.at(0).data(),outputs.at(0).size());
			
			// ignore our own messages
			if(sender_id == my_id) return true;
		}
		
		// normal broadcast message is 2 parts
		if(outputs.size()==2){
			
			// got a broadcast message format.
			++mm_broadcasts_recvd;  // FIXME this includes negotiation requests
			
			// in broadcast messages the second part is an integer
			// indicating whether that sender considers itself the master.
			// If there's a clash, we'll need to negotiate.
			uint32_t is_master = *(reinterpret_cast<uint32_t*>(outputs.at(1).data()));
			
			if(is_master && am_master){
				Log("Both middleman are masters! ...",3);
				need_to_negotiate = true;
			
			} else if(!is_master && !am_master){
				Log("Neither middlemen are masters! ...",3);
				// avoid unnecessary negotiation if we're fixed to being standby.
				// it's possible both are fixed to being standby
				// if not, the other standby will open negotiation eventually
				if(not dont_promote) need_to_negotiate = true;
			
			} else {
				if(need_to_negotiate){
					Log("...Disregarding stale role collision",3);
				}
				need_to_negotiate = false;
				their_header="";
				their_timestamp="";
			}
			
			last_mm_receipt = boost::posix_time::microsec_clock::universal_time();
			
		} else if(outputs.size()==3){
			
			// negotiation is done via the same socket, but involves 3-part messages.
			// if the other middleman has invoked negotiation, we may have received 3 parts.
			their_header.resize(outputs.at(1).size(),'\0');
			memcpy((void*)their_header.data(),outputs.at(1).data(),outputs.at(1).size());
			their_timestamp.resize(outputs.at(2).size(),'\0');
			memcpy((void*)their_timestamp.data(),outputs.at(2).data(),outputs.at(2).size());
			
			
			if(their_header=="Negotiate"){
				// it's a request to negotiate
				need_to_negotiate = true;
			
			} else if(their_header=="VerifyMaster" || their_header=="VerifyStandby"){
				// These suggest negotiation completed.
				if(need_to_negotiate){
					Log("...Disregarding stale role collision",3);
				}
				need_to_negotiate = false;
				their_header="";
				their_timestamp="";
				
			} else {
				Log(Concat("Unrecognised message header: '",their_header,"' from middleman"),0);
				// FIXME ignore it? would it be safer to negotiate, just to be sure?
				
			}
			
		} else {
			// else more than 3 parts
			Log(Concat("unexpected ",outputs.size()," part message from middleman"),0);
			++mm_broadcast_recv_fails; // FIXME this could include negotiation requests
			return false;
		}
		
		// but perhaps this message is stale:
		// re-poll the socket and see if there is another message in the buffer
		try {
			get_ok = zmq::poll(&in_polls.at(2), 1, 0);
		} catch (zmq::error_t& err){
			std::cerr<<"ReceiveSQL::GetMiddlemanCheckin poller caught "<<err.what()<<std::endl;
			get_ok = -1;
		}
		
	} // else no broadcast message from other middleman
	
	// see if we had any unresolved role collisions
	if(need_to_negotiate){
		
		if(am_master){
			Log("Both middlemen are masters! Starting negotiation",0);
			++master_clashes;
		} else {
			Log("Both middlemen are standbys! Starting negotiation",0);
			++standby_clashes;
		}
		
		get_ok = NegotiateMaster(their_header, their_timestamp);
		
		if(not get_ok){
			Log("Error negotiating master!",0);
			// ... what do we doo... XXX
			if(am_master) ++master_clashes_failed;
			else          ++standby_clashes_failed;
			return false;
		} else {
			Log(Concat("Successfully negotiated new role as ",(am_master ? "Master" : "Standby")),0);
		}
		
	}
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::CheckMasterStatus(){
	
	// check how long it's been since we last heard from the other middleman
	elapsed_time = promote_timeout - (boost::posix_time::microsec_clock::universal_time() - last_mm_receipt);
	
	if(elapsed_time.is_negative() && !am_master && !dont_promote){
		
		// Master's gone ... it's time for MUTINY!
		Log("Master has gone silent! Promoting to master",0);
		++self_promotions;
		am_master = true;
		get_ok = UpdateRole();
		if(not get_ok){
			// uhh, failed. :( Maybe the promotion timed out?
			++self_promotions_failed;
			return false;
		}
		
	} else if(elapsed_time.is_negative() && warn_no_standby){
		// if we're the master, it's not such a concern, but we may wish to log it?
		Log(Concat("Broadcast message from slave is overdue by ",elapsed_time.total_seconds()," seconds"),1);
		
	} else if(mm_warn_timeout > 0 && elapsed_time.total_milliseconds() > mm_warn_timeout){
		// log a warning if we're getting close
		Log(Concat("warning: ",elapsed_time.seconds()," seconds since last master check-in"),1);
		
	} // else other middleman has checked in, or i'm master
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::RunNextWriteQuery(){
	
	// run our next postgres query, if we have one
	if(wrt_txn_queue.size()){
		
		Query& next_msg = wrt_txn_queue.begin()->second;
		std::string& db = next_msg.database;
		if(m_databases.count(db)==0){
			Log("Write query to unknown database '"+db+"'",v_error);
			return false;
		}
		std::string err;
		next_msg.query_ok = m_databases.at(db).QueryAsJsons(next_msg.query, &next_msg.response, &err);
		if(not next_msg.query_ok){
			Log(Concat("Write query failed! Query was: \"",next_msg.query,"\", error was: '",err,"'"),1);
			++write_queries_failed;
			next_msg.response = std::vector<std::string>{err};
		}
		
		// push the response into the queue
		resp_queue.emplace(wrt_txn_queue.begin()->first, next_msg);
		// remove the query from the queue
		wrt_txn_queue.erase(wrt_txn_queue.begin());
		
	} // else no postgresql transactions to run
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::RunNextReadQuery(){
	
	// run our next postgres query, if we have one
	if(rd_txn_queue.size()){
		
		Query& next_msg = rd_txn_queue.begin()->second;
		std::string& db = next_msg.database;
		if(m_databases.count(db)==0){
			Log("Read query to unknown database '"+db+"'",v_error);
			return false;
		}
		std::string err;
		next_msg.query_ok = m_databases.at(db).QueryAsJsons(next_msg.query, &next_msg.response, &err);
		if(not next_msg.query_ok){
			Log(Concat("Read query failed! Query was: \"",next_msg.query,"\", error was: '",err,"'"),1);
			++read_queries_failed;
			next_msg.response = std::vector<std::string>{err};
		}
		
		// push the response into the queue
		resp_queue.emplace(rd_txn_queue.begin()->first, next_msg);
		// remove the query from the queue
		rd_txn_queue.erase(rd_txn_queue.begin()->first);
		
	} // else no postgresql transactions to run
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::RunNextLogMsg(){
	
	// insert our next log message, if we have one
	if(in_log_queue.size()){
		Log("Inserting next log message to DB: we have "+std::to_string(in_log_queue.size())
		    +" messages to process",5);
		
		std::string next_msg = in_log_queue.front();
		
		std::string tablename = "logging";
		get_ok = m_databases.at("daq").Query(next_msg);  // FIXME hard-coded db name
		
		if(not get_ok){
			// something went wrong
			std::cerr<<"Error inserting logmessage '"<<next_msg<<"' into database"<<std::endl;
			// can't use Log or we end up in a circular loop
			in_log_queue.pop_front();
			return false;
		}
		
		// remove the message from the queue
		in_log_queue.pop_front();
		
	} // else no log messages for now
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::RunNextMonitoringMsg(){
	
	// insert our next monitoring message, if we have one
	if(in_monitoring_queue.size()){
		Log("Inserting next monitoring message to DB: we have "+std::to_string(in_monitoring_queue.size())
		    +" messages to process",5);
		
		std::string next_msg = in_monitoring_queue.front();
		
		std::string tablename = "monitoring";
		get_ok = m_databases.at("daq").Query(next_msg);  // FIXME hard-coded db name
		
		if(not get_ok){
			// something went wrong
			Log("Error inserting logmessage '"+next_msg+"' into database",v_error);
			in_monitoring_queue.pop_front();
			return false;
		}
		
		// remove the message from the queue
		in_monitoring_queue.pop_front();
		
	} // else no monitoring messages for now
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::SendNextReply(){
	
	Log("Size of reply queue is "+std::to_string(resp_queue.size()),10);
	
	// send next response message, if we have one in the queue
	if(resp_queue.size()){
		
		// check we had a listener ready
		if(out_polls.at(1).revents & ZMQ_POLLOUT){
			
			// OK to send! Get the message to acknowledge
			Query& next_msg = resp_queue.begin()->second;
			// the ack message is a 3+ message
			// 1. the ZMQ_IDENTITY of the recipient, which will be stripped off and used by the ROUTER socket.
			// 2. the message ID, used by the client to match to the message it sent
			// 3. the response code, to signal errors
			// 4.... the SQL query results, if any. Each row is returned in a new message part.
			
			std::string client_str(reinterpret_cast<char*>(next_msg.client_id.data()));
			uint32_t* msgID = reinterpret_cast<uint32_t*>(next_msg.message_id.data());
			Log("Sending next reply to ZMQ IDENTITY '"+client_str+"' for msg "+std::to_string(*msgID),1);
			
			// as soon as we send a zmq::message_t (i.e. client_id and message_id), they are "used up":
			// the 'message.size()' becomes 0 and they strictly no longer retain their contents.
			// to keep a copy cached for re-sending we need to explicitly make a copy now.
			Query qrycpy(next_msg);  // (the Query copy-constructor invokes zmq::message_t->copy on members)
			
			if(next_msg.response.size()==0){
				try{
					get_ok = Send(clt_rtr_socket,
					              false,
					              next_msg.client_id,
					              next_msg.message_id,
					              next_msg.query_ok);
				} catch(std::exception& e){
					std::cerr<<"write caught "<<e.what()<<" sending with client id "<<client_str<<std::endl;
					get_ok = false;
				}
			} else {
				try{
					get_ok = Send(clt_rtr_socket,
					              false,
					              next_msg.client_id,
					              next_msg.message_id,
					              next_msg.query_ok,
					              next_msg.response);
				} catch(std::exception& e){
					std::cerr<<"read caught "<<e.what()<<" sending with client id "<<client_str<<std::endl;
					get_ok=false;
				}
			}
			
			if(get_ok){
				// all parts sent successfully, add to the sent cache
				cache.emplace(resp_queue.begin()->first,qrycpy);
				// remove from the to-send queue
				resp_queue.erase(resp_queue.begin()->first);
				++reps_sent;
				
			} else {
				Log("Error sending acknowledgement message!",1);
				if(next_msg.retries>=max_send_attempts){
					// give up
					resp_queue.erase(resp_queue.begin()->first);
					++rep_send_fails;
					return false;
				} else {
					++next_msg.retries;
				}
			} // end send ok check
			
		} // else no available listeners
		
	} // else no responses to send
/*
else {
	std::cout<<"no listeners on reply port"<<std::endl;
}
*/
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::SendNextLogMsg(){
	
	// send next logging message to the master, if we have one in the queue
	if(out_log_queue.size()){
		
		// check we had a listener ready
		if(out_polls.at(0).revents & ZMQ_POLLOUT){
			
			// OK to send! Get the message
			std::string& message = out_log_queue.front();
			
			int cnt = sendto(multicast_socket, message.c_str(), message.length()+1, 0, (struct sockaddr*)&multicast_addr, multicast_addrlen);
			if(cnt < 0){
				std::string errmsg = "Error sending multicast message: "+std::string{strerror(errno)};
				Log(errmsg,v_error);
				out_log_queue.pop_front();
				return false;
				
			} else {
				// sent successfully, remove from the to-send queue
				out_log_queue.pop_front();
				++log_msgs_sent;
				
			} // end send ok check
			
		} // else no available listeners
		
	} // else no log messages to send
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::BroadcastPresence(){
	
	// check if we need to broadcast our presence to the other middleman
	elapsed_time = broadcast_period - (boost::posix_time::microsec_clock::universal_time() - last_mm_send);
	
	if(elapsed_time.is_negative()){
		
		if(out_polls.at(2).revents & ZMQ_POLLOUT){
			
			++mm_broadcasts_sent;
			uint32_t msg = am_master;
			get_ok = Send(mm_snd_socket, false, my_id, msg);
			
			if(get_ok){
				last_mm_send = boost::posix_time::microsec_clock::universal_time();
				
			} else {
				Log("Error broadcasting middleman presence!",0);
				++mm_broadcasts_failed;
				return false;
			}
			
		} /*else { std::cout<<"no listeners"<<std::endl; }*/
		
	}  // else not yet.
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::CleanupCache(){
	
	// cleanup any old messages from the cache
	// to remove elements from a std::map while iterating through it, we can't use a range-based loop
	// instead do it this way:
	for(std::map<std::pair<std::string, uint32_t>, Query>::iterator it=cache.begin(); it!=cache.end(); ){
		Query& msg = it->second;
		elapsed_time =
		    cache_period - (boost::posix_time::microsec_clock::universal_time() - msg.recpt_time);
		
		if(elapsed_time.is_negative()){
			// drop from the cache
			it = cache.erase(it);
			// std::map::erase returns a new, valid iterator to the next element after that erased.
		} else {
			++it;
		}
	}
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::TrimQueue(const std::string& queuename){
	
	// check acknowledge queue size and do the same
	std::map<std::pair<std::string, uint32_t>, Query>* queue;
	unsigned long* drop_count;
	
	// check which queue we're managing
	if(queuename=="response_queue"){
		queue = &resp_queue;
		drop_count = &dropped_acks;
	} else if(queuename=="wrt_txn_queue"){
		queue = &wrt_txn_queue;
		drop_count = &dropped_writes;
	} else if(queuename=="rd_txn_queue"){
		queue = &rd_txn_queue;
		drop_count = &dropped_reads;
	} else {
		Log(Concat("TrimQueue called with unknown message queue '",queuename,"'"),0);
		return false;
	}
	
	// check if we need to drop anything
	if(queue->size() > drop_limit){
		int to_drop = queue->size() - drop_limit;
		Log(Concat("Warning! Number of waiting elements in queue ",queuename," (",queue->size(),
		           ") is over limit (",drop_limit,")! Dropping ",to_drop," messages!"),0);
		for(int i=0; i<to_drop; ++i){ queue->erase(queue->begin()->first); }
		*drop_count += to_drop;
		
	// check if we need to warn about being close to the limit
	} else if(queue->size() > warn_limit){
		Log(Concat("Warning! Number of waiting elements in ",queuename," (",queue->size(),
		           ") is approaching drop limit (",drop_limit,")!",
		           "Is the network down, or responding slowly?"),1);
	
	}
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::TrimDequeue(const std::string& queuename){
	
	// check in or out log message queue size and do the same
	std::deque<std::string>* queue;
	unsigned long* drop_count;
	
	// check which queue we're managing
	if(queuename=="in_log_queue"){
		queue = &in_log_queue;
		drop_count = &dropped_logs_out;
	} else if(queuename=="out_log_queue"){
		queue = &out_log_queue;
		drop_count = &dropped_logs_in;
	} else if(queuename=="in_monitoring_queue"){
		queue = &in_monitoring_queue;
		drop_count = &dropped_monitoring_in;
	} else {
		Log(Concat("TrimDequeue called with unknown message queue '",queuename,"'"),0);
		return false;
	}
	
	// check if we need to drop anything
	if(queue->size() > drop_limit){
		int to_drop = queue->size() - drop_limit;
		Log(Concat("Warning! Number of waiting elements in queue ",queuename," (",queue->size(),
		           ") is over limit (",drop_limit,")! Dropping ",to_drop," messages!"),0);
		for(int i=0; i<to_drop; ++i){ queue->pop_front(); }
		*drop_count += to_drop;
		
	// check if we need to warn about being close to the limit
	} else if(queue->size() > warn_limit){
		Log(Concat("Warning! Number of waiting elements in ",queuename," (",queue->size(),
		           ") is approaching drop limit (",drop_limit,") !",
		           "Is the network down, or responding slowly?"),1);
	}
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::TrimCache(){
	
	// check cache size and do the same
	if(cache.size() > drop_limit){
		
		int to_drop = cache.size() - drop_limit;
		
		Log(Concat("Warning! Number of cached reponses is over limit!",
		           "Dropping ",to_drop," responses!"),0);
		
		// drop the oldest X messages. To do this we need to sort the cache by receipt time.
		std::map<boost::posix_time::ptime,
		         std::map<std::pair<std::string, uint32_t>, Query>::iterator> sorted_cache;
		
		// fill the sorted map
		for(auto it = cache.begin(); it!=cache.end(); ++it){
			sorted_cache.emplace(it->second.recpt_time, it);
		}
		
		// drop the oldest messages
		for(int i=0; i<to_drop; ++i){
			cache.erase(sorted_cache.begin()->second);
		}
		
	// else check if we need to warn about being close to the limit
	} else if(resp_queue.size() > warn_limit){
		Log(Concat("Warning! Number of cached responses is approaching limit!",
		           "Reduce cache period or increase cache max size?"),1);
	}
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::UpdateControls(){
	// we need to check all the registered controls for updates
	// (this is expected to change when we can register callbacks, so we'll implement with that in mind)
	bool stop=false;
	SC_vars["Restart"]->GetValue(stop);
	if(stop) DoStop(stop);
	
	bool quit=false;
	SC_vars["Quit"]->GetValue(quit);
	if(quit) DoQuit(quit);
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::TrackStats(){
	
	// get values from last stats dump so we can see how much we've accumulated since then
	elapsed_time = stats_period - (boost::posix_time::microsec_clock::universal_time() - last_stats_calc);
	
	if(elapsed_time.is_negative()){
		
		// to calculate rates we need to know the difference in number
		// of reads/writes since last time. So get the last values
		unsigned long last_write_query_count;
		unsigned long last_read_query_count;
		MonitoringStore.Get("write_queries_recvd", last_write_query_count);
		MonitoringStore.Get("read_queries_recvd", last_read_query_count);
		
		// calculate rates are per minute
		elapsed_time = boost::posix_time::microsec_clock::universal_time() - last_stats_calc;
		
		float read_query_rate = (elapsed_time.total_seconds()==0) ? 0 :
		    ((read_queries_recvd - last_read_query_count) * 60.) / elapsed_time.total_seconds();
		float write_query_rate = (elapsed_time.total_seconds()==0) ? 0 :
		    ((write_queries_recvd - last_write_query_count) * 60.) / elapsed_time.total_seconds();
		
		// dump all stats into a Store.
		MonitoringStore.Set("write_queries_waiting",wrt_txn_queue.size());
		MonitoringStore.Set("read_queries_waiting",rd_txn_queue.size());
		MonitoringStore.Set("replies_waiting",resp_queue.size());
		MonitoringStore.Set("incoming_logs_waiting",in_log_queue.size());
		MonitoringStore.Set("outgoing_log_waiting",out_log_queue.size());
		MonitoringStore.Set("incoming_monitoring_waiting",in_monitoring_queue.size());
		MonitoringStore.Set("cached_queries",cache.size());
		MonitoringStore.Set("write_queries_recvd", write_queries_recvd);
		MonitoringStore.Set("write_query_recv_fails", write_query_recv_fails);
		MonitoringStore.Set("read_queries_recvd", read_queries_recvd);
		MonitoringStore.Set("read_query_recv_fails", read_query_recv_fails);
		MonitoringStore.Set("log_msgs_recvd", log_msgs_recvd);
		MonitoringStore.Set("log_msg_recv_fails", log_msg_recv_fails);
		MonitoringStore.Set("mm_broadcasts_recvd", mm_broadcasts_recvd);
		MonitoringStore.Set("mm_broadcast_recv_fails", mm_broadcast_recv_fails);
		MonitoringStore.Set("write_queries_failed", write_queries_failed);
		MonitoringStore.Set("read_queries_failed", read_queries_failed);
		MonitoringStore.Set("in_logs_failed", in_logs_failed);
		MonitoringStore.Set("reps_sent", reps_sent);
		MonitoringStore.Set("rep_send_fails", rep_send_fails);
		MonitoringStore.Set("log_msgs_sent", log_msgs_sent);
		MonitoringStore.Set("log_send_fails", log_send_fails);
		MonitoringStore.Set("mm_broadcasts_sent", mm_broadcasts_sent);
		MonitoringStore.Set("mm_broadcasts_failed", mm_broadcasts_failed);
		MonitoringStore.Set("master_clashes", master_clashes);
		MonitoringStore.Set("master_clashes_failed", master_clashes_failed);
		MonitoringStore.Set("standby_clashes", standby_clashes);
		MonitoringStore.Set("standby_clashes_failed", standby_clashes_failed);
		MonitoringStore.Set("self_promotions", self_promotions);
		MonitoringStore.Set("self_promotions_failed", self_promotions_failed);
		MonitoringStore.Set("promotions", promotions);
		MonitoringStore.Set("promotions_failed", promotions_failed);
		MonitoringStore.Set("demotions", demotions);
		MonitoringStore.Set("demotions_failed", demotions_failed);
		MonitoringStore.Set("dropped_writes", dropped_writes);
		MonitoringStore.Set("dropped_reads", dropped_reads);
		MonitoringStore.Set("dropped_acks", dropped_acks);
		MonitoringStore.Set("dropped_logs_in", dropped_logs_in);
		MonitoringStore.Set("dropped_logs_out", dropped_logs_out);
		MonitoringStore.Set("dropped_monitoring_in", dropped_monitoring_in);
		MonitoringStore.Set("dropped_monitoring_out", dropped_monitoring_out);
		MonitoringStore.Set("read_query_rate", read_query_rate);
		MonitoringStore.Set("write_query_rate", write_query_rate);
		
		// convert Store into a json
		std::string json_stats;
		MonitoringStore >> json_stats;
		
		// update the web page status
		// actually, this only supports a single word, with no spaces?
		std::stringstream status;
		status << "  r:["<<read_queries_recvd<<"|"<<read_query_recv_fails<<"|"<<read_queries_failed
		       <<"]; w:["<<write_queries_recvd<<"|"<<write_query_recv_fails<<"|"<<write_queries_failed
		       <<"]; l:["<<log_msgs_recvd<<"|"<<log_msg_recv_fails<<"|"<<in_logs_failed
		       <<"]; a:["<<reps_sent<<"|"<<rep_send_fails
		       <<"]; d:["<<dropped_reads<<"|"<<dropped_writes<<"|"<<dropped_logs_in<<"|"<<dropped_acks
		       <<"]";
		SC_vars["Status"]->SetValue(status.str());
		
		// temporarily bypass the database logging level to ensure it gets sent to the monitoring db.
		int db_verbosity_tmp = db_verbosity;
		db_verbosity = 10;
		Log(Concat("Monitoring Stats:",json_stats),5);
		db_verbosity = db_verbosity_tmp;
		
		last_stats_calc = boost::posix_time::microsec_clock::universal_time();
	}
	
	return true;
}

//                   ≫ ──── ≪•◦ ❈ ◦•≫ ──── ≪
//                        Support Routines
//                   ≫ ──── ≪•◦ ❈ ◦•≫ ──── ≪

bool ReceiveSQL::NegotiateMaster(const std::string& their_header, const std::string& their_timestamp){
	
	// we need to establish who's the master.
	// The master will be decided based on who has the most recently modified datbase.
	
	// 1. We send <"our_ID"> <"Negotiate"> <timestamp>
	// 2. Other compares this timestamp with when their database was last modified
	// 3. Other middleman responds <"their_ID"> <"VerifyMaster"/"VerifyStandby"> <timestamp>
	// 4. We take on the other role than they indicated.
	//    Optionally we may verify their decision by comparing their returned timestamp against ours.
	
	// invoke the appropriate action based on who initiated negotiation:
	if(their_header==""){
		// we did
		get_ok = NegotiationRequest();
	} else {
		// they did
		get_ok = NegotiationReply(their_header, their_timestamp);
	}
	return get_ok;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::NegotiationRequest(){
	
	std::string our_header="Negotiate";
	std::string msg_type; // header of received response
	bool was_master = am_master; // our role before negotiation

	// get the last update time of our database
	std::string our_timestamp;
	get_ok = GetLastUpdateTime(our_timestamp);
	if(not get_ok){
		// uh-oh, couldn't get our last updated time....
		// error will already have been logged, but what do we do now? XXX
		return false;
	}
	
	// we'll re-send intermittently if we don't get a response.
	boost::posix_time::ptime last_send = boost::posix_time::microsec_clock::universal_time();
	boost::posix_time::time_duration send_period = boost::posix_time::milliseconds(negotiate_period);
	boost::posix_time::time_duration elapsed_time;
	// immediately send the first message
	bool first_send = true;
	
	// If we don't manage to contact the other middleman in a while, abandon negotiation and assume mastership
	boost::posix_time::ptime negotiation_start = boost::posix_time::microsec_clock::universal_time();
	boost::posix_time::time_duration negotiation_time = boost::posix_time::milliseconds(negotiation_timeout);
	
	// ok, start transmission
	do {
		
		elapsed_time = send_period - (boost::posix_time::microsec_clock::universal_time() - last_send);
		if(first_send || elapsed_time.is_negative()){
			
			// send out our message
			int ret = PollAndSend(mm_snd_socket, out_polls.at(2), outpoll_timeout, my_id, our_header, our_timestamp);
			
			// check for errors
			if(ret==-3) Log("Error polling out socket in NegotiateMaster() call!",0);
			if(ret==-2) Log("No listener on out socket in NegotiateMaster() call!",0);
			if(ret==-1) Log("Error broadcasting negotiation request!",0);
			if(ret!=0){
				// uh-oh, couldn't send our response....
				// error will have already been logged, but what do we do now? XXX
				++mm_broadcasts_failed;
				return false;
			}
			
		}
		
		// receive the other middleman's response
		std::vector<zmq::message_t> messages;
		
		int ret = PollAndReceive(mm_rcv_socket, in_polls.at(2), inpoll_timeout, messages);
		
		// chech for errors
		if(ret==-3) Log("Error polling in socket in NegotiateMaster() call!",0);
		//if(ret==-2) Log("No waiting messages on socket in NegotiateMaster() call!",3); // not an error.
		if(ret==-1) Log("Error receiving negotiation message!",0);
		if(ret!=0 && ret!=-2){
			// uh-oh, couldn't send our response....
			// error will have already been logged, but what do we do now? XXX
			++mm_broadcast_recv_fails;
			return false;
		}
		
		// if no errors, handle response
		if(ret==0){
			// we got a message
			if(messages.size()>0){
				// first part is the sender id
				std::string sender_id(messages.at(0).size(),'\0');
				memcpy((void*)sender_id.data(),messages.at(0).data(),messages.at(0).size());
				
				// ignore our own messages
				if(sender_id == my_id) continue;
			}
			
			// 2 part message is probably got a normal broadcast message. process it.
			if(messages.size()==2){
				uint32_t is_master = *(reinterpret_cast<uint32_t*>(messages.back().data()));
				if((is_master && am_master) || (!is_master && !am_master)){
					// we need to negotiate - we're aready doing it!
				} else {
					Log("Unexpected master resolution during negotiation",0);
					// uhhh, we started negotiating, but somehow we're no longer in conflict?
					// well we've started so let's finish
				}
				last_mm_receipt = boost::posix_time::microsec_clock::universal_time();
			}
			
			// more than 3 parts is an invalid message format. discard it.
			else if(messages.size()>3){
				Log(Concat("Unexpected ",messages.size()," part message from middleman"),0);
			}
			
			// else 3 parts - the expected format is an id, a header, and a timestamp.
			else {
				
				// unpack the message
				msg_type.resize(messages.at(1).size(),'\0');
				memcpy((void*)msg_type.data(),messages.at(1).data(),messages.at(1).size());
				std::string their_timestamp(messages.at(2).size(),'\0');
				memcpy((void*)their_timestamp.data(),messages.at(2).data(),messages.at(2).size());
				
				// ensure it's one we recognise
				if(msg_type=="Negotiate" || msg_type=="VerifyMaster" || msg_type=="VerifyStandby"){
					
					// determine our new role
					am_master = (ToTimestamp(our_timestamp)>ToTimestamp(their_timestamp));
					our_header = (am_master) ? "VerifyMaster" : "VerifyStandby";
					
					// check consistency with their derived role
					if(msg_type==our_header){
						// uh-oh. Somehow we both came to the conclusion that we're the same role???
						Log(Concat("Negotiation resulted in both middlemen claiming role ",
						          (am_master ? "Master" : "Standby")),0);
						// what do we do now? XXX
						return false;
					}
					
					// otherwise, time to break our transmission loop.
					break;
					
				} else {
					// unknown message header received
					Log(Concat("Unknown negotiation message type '",msg_type,"'"),0);
					// ... umm, i guess we ignore it...
				}
				
			}  // end of cases on number of received message parts
			
		} // else no message from other middleman
		
		// check how long we're been trying to negotiate
		elapsed_time =
		    negotiation_time - (boost::posix_time::microsec_clock::universal_time() - negotiation_start);
		
	} while (!elapsed_time.is_negative());
	
	
	// check if we timed out trying to negotiate
	if(elapsed_time.is_negative()){
		// the other middleman went silent. we'll take mastership.
		Log("Timeout during negotiation! Assuming master role",0);
		am_master = true;
	}
	
	// assume our new role
	get_ok = true;
	if(was_master != am_master) get_ok = UpdateRole();
	if(not get_ok){
		// uhh, failed. :( Maybe the promotion/demotion timed out?
		return false;
	}
	
	// check what kind of message we received - we may need to send a response
	if(msg_type=="Negotiate"){
		// they must've opened negotiations at the same time we did. They may be expecting a reply.
		
		// send the reply
		int ret = PollAndSend(mm_snd_socket, out_polls.at(2), outpoll_timeout, my_id, our_header, our_timestamp);
		
		// handle errors
		if(ret==-3) Log("Error polling out socket in NegotiateMaster() call!",0);
		if(ret==-2) Log("No listener on out socket in NegotiateMaster() call!",0);
		if(ret==-1) Log("Error broadcasting negotiation request!",0);
		if(ret!=0){
			// uh-oh, couldn't send our response....
			// error will have already been logged, but what do we do now? XXX
			++mm_broadcasts_failed;
			return false;
		}
		
	}
	// nothing else to do, negotiation complete.
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::NegotiationReply(const std::string& their_header, const std::string& their_timestamp){
	
	// other side of negotiations. This is simpler since we already have the other middleman's
	// timestamp, so all we need to do is deduce our new role and send the response.
	
	// get the last update time of our database
	std::string our_timestamp;
	get_ok = GetLastUpdateTime(our_timestamp);
	
	if(not get_ok){
		// uh-oh, couldn't get our last updated time....
		// error will already have been logged, but what do we do now? XXX
		return false;
	}
	
	// based on this and the received timestamp, determine our role and respond.
	bool was_master = am_master;
	am_master = (ToTimestamp(our_timestamp)>ToTimestamp(their_timestamp));
	std::string our_header = (am_master) ? "VerifyMaster" : "VerifyStandby";
	
	// before we change role, we should check if we're supposed to be locked as standby
	if(am_master && dont_promote){
		// unless the other middleman is also set to not promote, it will keep trying
		// to negotiate a new master, but we cannot assume this role! Set the other middleman
		// 'dont_promote' to true to prevent endless negotiation requests
		std::string logmessage = "Negotiation requested for a middleman locked to standby!";
		Log(logmessage,1);
	}
	
	// if we changed role, try to promote/demote ourselves to the new role
	// before we respond to the other middleman (in case of error)
	if(am_master != was_master) get_ok = UpdateRole();
	if(not get_ok){
		// uhh, failed. :( Maybe the promotion/demotion timed out?
		return false;
	}
	
	// inform the other middleman
	int ret = PollAndSend(mm_snd_socket, out_polls.at(2), 500, my_id, our_header, our_timestamp);
	
	// handle errors
	if(ret==-3) Log("Error polling out socket in NegotiateMaster() call!",0);
	if(ret==-2) Log("No listener on out socket in NegotiateMaster() call!",0);
	if(ret==-1) Log("Error broadcasting negotiation request!",0);
	if(ret!=0){
		// uh-oh, couldn't send our response....
		// error will have already been logged, but what do we do now? XXX
		++mm_broadcasts_failed;
		return false;
	}
	
	// nothing else to do, negotiation complete.
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::UpdateRole(){
	
	if(am_master){
		++promotions;
		// connect to the write query port and logging ports to ensure we can assume master role.
		
		// socket to receive published write queries from clients
		// -------------------------------------------------------
		clt_sub_socket = new zmq::socket_t(*context, ZMQ_SUB);
		// this socket never sends, so a send timeout is irrelevant.
		clt_sub_socket->setsockopt(ZMQ_RCVTIMEO, clt_sub_socket_timeout);
		// don't linger too long, it looks like the program crashed.
		clt_sub_socket->setsockopt(ZMQ_LINGER, 10);
		// connections to clients will be made via the utilities class
		clt_sub_socket->setsockopt(ZMQ_SUBSCRIBE,"",0);
		
		// add the additional poll for this socket
		in_polls.emplace_back(zmq::pollitem_t{*clt_sub_socket,0,ZMQ_POLLIN,0});
		
		// promote the database out of recovery mode. 60s timeout.
		std::string err;
		get_ok = m_databases.begin()->second.Promote(60,&err); // FIXME multiple dbs?
		
		// should we also stop broadcasting ourself as a source of logging messages?
		utilities->RemoveService("logging");
		
		// check for errors
		if(get_ok){
			Log("Promotion success",1);
			
		} else {
			
			// uh-oh, error. Try to run it back.
			++promotions_failed;
			Log(Concat("Promotion failed! Error was: ",err),0);
			am_master = false;  // properly reflect our current status
			// XXX are we sure this is our true status?? FIXME query db to check.
			
			// disconnect from the write ports again
			delete clt_sub_socket; clt_sub_socket=nullptr;
			// remove the polls
			in_polls.pop_back();
			in_polls.pop_back();
			
			return false;
		}
		
		// end of promotion
		
	} else {
		// else our new role is standby
		
		++demotions;
		std::string err;
		
		// demote the database to standby. 60s timeout.
		get_ok = m_databases.begin()->second.Demote(60,&err); // FIXME multiple dbs?
		
		// check for errors
		if(get_ok){
			Log("Demotion success",1);
			
			// disconnect from the write message ports so we don't get those messages
			delete clt_sub_socket; clt_sub_socket=nullptr;
			// remove the associated polls
			in_polls.pop_back();
			in_polls.pop_back();
			
			// drop any outstanding write messages
			wrt_txn_queue.clear();
			in_log_queue.clear();
			
		} else {
			
			// uh-oh. something went wrong.
			++demotions_failed;
			Log(Concat("Demotion failed! Error was: ",err),0);
			
			// maybe we're still master....???
			// am_master = true;  // properly reflect our current status
			// FIXME query database to check if this is our true status
			
			// or more likely the database is now down due to a synchronization error!!
			// FIXME check if database is still up. if not... we need manual intervention here!!!
			
			return false;
			
		} // end if/else error demoting
		
	} // else new role master/standby switch
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

// to compare timestamp strings returned from "pg_last_committed_xact"
// we need to turn the string into a proper comparable boost::posix_time::ptime.
boost::posix_time::ptime ReceiveSQL::ToTimestamp(const std::string& timestring){
	
	// convert postgres timestamp string to a timestamp we can compare
	// postgres timestamps are in the format "2015-10-02 11:16:34.678267+01"
	// the trailing "+01" is number of hours in local timezone relative to UTC
	
	// we can parse the string into a time variable using a std::tm struct
	std::tm mytm = {0};
	sscanf(timestring.c_str(),"%4d-%2d-%2d %2d:%2d:%2d",
	       &mytm.tm_year, &mytm.tm_mon, &mytm.tm_mday, &mytm.tm_hour, &mytm.tm_min, &mytm.tm_sec);
	
	// months need correcting to 0-based indexing
	--mytm.tm_mon;
	// year needs correcting to 1900-based yearing
	mytm.tm_year-=1900;
	
	// we can form the boost::ptime from this struct
	return boost::posix_time::ptime_from_tm(mytm);
	
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

// to insert boost timestamps we need to turn them into Postgres strings
std::string ReceiveSQL::ToTimestring(boost::posix_time::ptime timestamp){
	
	// convert boost timestamp to time struct
	// postgres timestamps are *printed* in the format "2015-10-02 11:16:34.678267+01"
	// the trailing "+01" is number of hours in local timezone relative to UTC
	// but they are *input* in ISO 8601 - i.e. 'YYYY-MM-DD HH:MM:SS.PPP TZ'
	// where PPP is up to 6 fractional seconds, and TZ is an optional timezone
	// (e.g. 'PST', 'Z' (i.e. UTC), or an offset from UTC '-8' for 8 hours ahead)
	// (https://www.postgresql.org/docs/current/datatype-datetime.html#datatype-datetime-input)
	
	// FIXME how do we get this? do we need to add it?
	
	// we can form time strings of our desired format most easily using a time struct
	// which we can make from the boost::ptime
	std::tm mytm = boost::posix_time::to_tm(timestamp);
	
	// put it into our string
	char timestring[20];
	sprintf(timestring, "%04d-%02d-%02d %02d:%02d:%02d",
	        mytm.tm_year + 1900,
	        mytm.tm_mon + 1,
	        mytm.tm_mday,
	        mytm.tm_hour,
	        mytm.tm_min,
	        mytm.tm_sec);
	
	return std::string(timestring);
	
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::GetLastUpdateTime(std::string& our_timestamp){
	
	// To get the timestamp of the last committed transaction we can use pg_last_committed_xact().
	// n.b. this requires 'track_commit_timestamp' is enabled in postgresql.conf
	
	std::string query = "SELECT * FROM pg_last_committed_xact();";
	std::string err;
	std::vector<std::string> results;
	
	bool query_ok = m_databases.begin()->second.QueryAsStrings(query, &results, 'r', &err); // FIXME multiple dbs?
	
	if(not query_ok || results.size()==0){
		Log(Concat("Error getting last commit timestamp in negotiation! ",
		           "Error was: ",err,", query returned ",results.size()," rows"),0);
		return false;
	}
	
	// query should have returned a one row, one field result, which is our timestamp.
	our_timestamp = results.front();
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

std::string ReceiveSQL::escape_json(std::string s){
	// https://stackoverflow.com/a/27516892
	
	// i think the only thing we really need to worry about escaping here are double quotes,
	// which would prematurely terminate the string, and backslash, which may result in
	// the backslash and a subsequent character being converted to a special character;
	// e.g. '\n' -> line feed, or 0x5C 0x6E -> 0x0A, which alters the user's string.
	
	size_t pos=0;
	do {
		pos = s.find("\\",pos);
		if(pos!=std::string::npos){
			s.insert(pos,"\\");
			pos+=2;
		}
	} while(pos!=std::string::npos);
	
	pos=0;
	do {
		pos = s.find('"',pos);
		if(pos!=std::string::npos){
			s.insert(pos,"\\");
			pos+=2;
		}
	} while(pos!=std::string::npos);
	
	return s;
}

//                   ≫ ──── ≪•◦ ❈ ◦•≫ ──── ≪
//                      generic ZMQ wrappers
//                   ≫ ──── ≪•◦ ❈ ◦•≫ ──── ≪

// ««-------------- ≪ °◇◆◇° ≫ --------------»»
bool ReceiveSQL::Send(zmq::socket_t* sock, bool more, zmq::message_t& message){
	bool send_ok;
	if(more) send_ok = sock->send(message, ZMQ_SNDMORE);
	else     send_ok = sock->send(message);
	return send_ok;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::Send(zmq::socket_t* sock, bool more, const std::string& messagedata){
	// form the zmq::message_t
	zmq::message_t message(messagedata.size());
	memcpy(message.data(), messagedata.data(), messagedata.size());
	//snprintf((char*)message.data(), messagedata.size()+1, "%s", messagedata.c_str());
	
	// send it with given SNDMORE flag
	bool send_ok;
	if(more) send_ok = sock->send(message, ZMQ_SNDMORE);
	else     send_ok = sock->send(message);
	
	return send_ok;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::Send(zmq::socket_t* sock, bool more, const std::vector<std::string>& messages){
	
	// loop over all but the last part in the input vector,
	// and send with the SNDMORE flag
	for(int i=0; i<(messages.size()-1); ++i){
		
		// form zmq::message_t
		zmq::message_t message(messages.at(i).size());
		memcpy(message.data(), messages.at(i).data(), messages.at(i).size());
		//snprintf((char*)message.data(), messages.at(i).size()+1, "%s", messages.at(i).c_str());
		
		// send this part
		bool send_ok = sock->send(message, ZMQ_SNDMORE);
		
		// break on error
		if(not send_ok) return false;
	}
	
	// form the zmq::message_t for the last part
	zmq::message_t message(messages.back().size());
	memcpy(message.data(), messages.back().data(), messages.back().size());
	//snprintf((char*)message.data(), messages.back().size()+1, "%s", messages.back().c_str());
	
	// send it with, or without SNDMORE flag as requested
	bool send_ok;
	if(more) send_ok = sock->send(message, ZMQ_SNDMORE);
	else     send_ok = sock->send(message);
	
	return send_ok;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

int ReceiveSQL::PollAndReceive(zmq::socket_t* sock, zmq::pollitem_t poll, int timeout, std::vector<zmq::message_t>& outputs){
	
	// poll the input socket for messages
	try {
		get_ok = zmq::poll(&poll, 1, timeout);
	} catch (zmq::error_t& err){
		std::cerr<<"ReceiveSQL::PollAndReceive poller caught "<<err.what()<<std::endl;
		get_ok = -1;
	}
	if(get_ok<0){
		// error polling - is the socket closed?
		return -3;
	}
	
	// check for messages waiting to be read
	if(poll.revents & ZMQ_POLLIN){
		
		// recieve all parts
		get_ok = Receive(sock, outputs);
		if(not get_ok) return -1;
		
	} else {
		// no waiting messages
		return -2;
	}
	// else received ok
	return 0;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::Receive(zmq::socket_t* sock, std::vector<zmq::message_t>& outputs){
	
	outputs.clear();
	int part=0;
	
	// recieve parts into tmp variable
	zmq::message_t tmp;
	//std::cout<<"Receiving part ";
	bool err=false;
	while(true){
		//std::cout<<part<<"...";
		int ok = sock->recv(&tmp);
		if(ok<0){
			err=true;
			break;
		}
		// transfer the received message to the output vector
		outputs.resize(outputs.size()+1);
		outputs.back().move(&tmp);
		
		// receive next part if there is more to come
		if(!outputs.back().more()) break;
		++part;
	}
	//std::cout<<std::endl;
	
	if(err){
		// sock->recv failed
		//Log("Error receving zmq message!",0);  // log at caller for better context
		return false;
	}
	
	// otherwise no more parts. done.
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::Log(const std::string& message, uint32_t message_severity){
	
	// log locally, if within printout verbosity
	if(message_severity < stdio_verbosity){
		if(message_severity<2){
			std::cerr << message << std::endl;
		} else {
			std::cout << message << std::endl;
		}
	}
	
	// logging severity is limited to 0-10, coerce to range
	if(message_severity>10) message_severity=10;
	
	// make a timestamp
	std::string timestring = ToTimestring(boost::posix_time::microsec_clock::universal_time());
	
	// log to database, if within database logging verbosity
	if(message_severity < db_verbosity){
		
		// we'll either want to run this locally, or send it to the master, depending on our role
		if(am_master){
			// queue up for logging to our local monitoring database
			
			// SQL sanitization
			std::string msg;
			get_ok = m_databases.begin()->second.pqxx_quote(message, msg);
			if(!get_ok){
				std::cerr<<"Error sanitizing log message '"<<message<<"'"<<std::endl;
				// can't use Log here as the embedded message would result in a circular loop!
				return false;
			}
			
			// form the required SQL
			std::string logmsg = "INSERT INTO logging ( time, device, severity, message ) VALUES ( '"
			                   + timestring                       + "','"
			                   + my_id                            + "',"
			                   + std::to_string(message_severity) + ","
			                   + msg                              + ");";
			
			in_log_queue.emplace_back(logmsg);
			
		} else {
			// add to the queue of logging messages to send to the master over ZMQ
			
			// form the required JSON
			std::string logmsg = "{ \"time\":"+std::to_string(time(nullptr))
			                   +", \"device\":\""+escape_json(my_id)+"\""
			                   +", \"severity\":"+std::to_string(message_severity)
			                   +", \"message\":\""+escape_json(message)+"\" }";
			
			out_log_queue.emplace_back(logmsg);
		}
		
	} // else outside db logging verbosity
	
	return true;
}

// ««-------------- ≪ °◇◆◇° ≫ --------------»»

bool ReceiveSQL::DoStop(bool stop){
	if(stop){
		// make stop flag file which will trigger finalise and termination
		std::string cmd = "touch "+stopfile;
		std::system(cmd.c_str());
		SC_vars["Restart"]->SetValue(false);
		SC_vars["Status"]->SetValue("Stopping");
	}
	return true;
}

bool ReceiveSQL::DoQuit(bool quit){
	if(quit){
		// make stop flag file to stop this executable
		DoStop(true);
		// make quit flag file to prevent run_middleman.sh re-starting us
		std::string cmd = "touch "+quitfile;
		std::system(cmd.c_str());
		SC_vars["Quit"]->SetValue(false);
		SC_vars["Status"]->SetValue("Quitting");
	}
	return true;
}

//https://wiki.postgresql.org/wiki/What%27s_new_in_PostgreSQL_9.5#Commit_timestamp_tracking
//https://stackoverflow.com/questions/33943524/atomically-set-serial-value-when-committing-transaction/33944402#33944402
//https://dba.stackexchange.com/questions/199290/get-last-modified-date-of-table-in-postgresql
//https://dba.stackexchange.com/questions/123145/how-to-view-tuples-changed-in-a-postgresql-transaction/123183#123183
//https://stackoverflow.com/questions/56961111/questions-about-postgres-track-commit-timestamp-pg-xact-commit-timestamp
//https://newbedev.com/how-to-find-out-when-data-was-inserted-to-postgres

