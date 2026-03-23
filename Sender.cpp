#include <iostream>
#include <atomic>
#include <functional>
#include <future>
#include <valarray>
#include <sstream>
#include <chrono>
#include <pqxx/pqxx>
#include <DAQInterface.h>

using namespace ToolFramework;

struct MyThreadArgs {
	//MyThreadArgs(){};
	Store* arg_store=nullptr;
	DAQInterface* DAQ_inter=nullptr;
	bool running=false;
};

/*
static std::atomic<unsigned long long> thread_counter=0;
unsigned long long thread_id() {
	thread_local unsigned long long tid = thread_counter++;
	return tid;
}
*/

class Logger {
	public:
	Logger(int verbose=1) : m_verbose(verbose){};
	
	int m_verbose;
	void SetVerbosity(int verbose){ m_verbose=verbose; }
	
	std::ostream nullstream{nullptr};
	std::ostream& operator()(int msg_verb){
		if(msg_verb>m_verbose) return std::ref(nullstream);
		return std::ref(std::cout);
	}
};
enum verbosity { v_error=0, v_warning=1, v_message=2, v_debug=3 };

class SendManager{
	public:
	SendManager(DAQInterface* in_DAQ_inter, int verbose=1){
		DAQ_inter=in_DAQ_inter;
		m_verbose = verbose;
		m_log.SetVerbosity(m_verbose);
	};
	DAQInterface* DAQ_inter;
	int m_verbose=1;
	Logger m_log;
	MyThreadArgs logger_args;
	std::vector<std::future<std::valarray<int>>> logger_threads;
	
	
	// ==================================================================//
	// Main callback for creating a set of sender threads
	// ==================================================================//
	std::string StartLogging(const char* payload){
		
		m_log(v_message)<<"StartLogging called"<<std::endl;
		
		if(logger_threads.size()){
			std::string resp="StartLogging called but LogSender already running!";
			std::cerr<<resp<<std::endl;
			return resp;
		}
		
		Store arg_store;
		arg_store.JsonParser(payload);
		logger_args.arg_store = &arg_store;
		logger_args.DAQ_inter=DAQ_inter;
		logger_args.running=false;
		
		// -------------//
		// Sanity Checks
		// -------------//
		m_log(v_message)<<"performing sanity checks"<<std::endl;
		
		// check we have a valid stop condition
		int n_msgs, duration_ms;
		bool stop_signal=false;
		arg_store.Get("n_msgs",n_msgs);
		arg_store.Get("duration_ms",duration_ms);
		arg_store.Get("stop_signal",stop_signal);
		if(n_msgs<0 && duration_ms<0 && !stop_signal){
			std::string ret="no stop condition specified! "
			                "Please set either n_msgs, duration_ms or stop_signal";
			std::cerr<<ret<<std::endl;
			return ret;
		}
		
		// check we have valid message length specs
		int msg_length=100, msg_length_min=1, msg_length_max=100;
		bool randomize_msg=false;
		arg_store.Get("msg_length",msg_length);
		arg_store.Get("msg_length_min",msg_length_min);
		arg_store.Get("msg_length_max",msg_length_max);
		if(msg_length_max<msg_length_min) std::swap(msg_length_min,msg_length_max);
		arg_store.Get("randomize_msg",randomize_msg);
		if( (!randomize_msg && msg_length<=0) ||
		    (randomize_msg && ( (msg_length_min>0 && msg_length_max<=0) || (msg_length_min<=0 && msg_length_max>0) ) ||
		                      ( (msg_length_min<=0 && msg_length_min==msg_length_max) && msg_length<=0) )
		  ){
			std::string ret="no valid msg_length specified";
			std::cerr<<ret<<std::endl;
			return ret;
		}
		
		// check we have valid message rate
		int period_ms=100, period_ms_min=1, period_ms_max=100;
		bool randomize_period=false;
		arg_store.Get("period_ms",period_ms);
		arg_store.Get("period_ms_min",period_ms_min);
		arg_store.Get("period_ms_max",period_ms_max);
		arg_store.Get("randomize_period",randomize_period);
		if(period_ms_max<period_ms_min) std::swap(period_ms_min,period_ms_max);
		if( randomize_period && (period_ms_min<0 || period_ms_min==period_ms_max)
		  ){
			std::string ret="no valid message rate specified";
			std::cerr<<ret<<std::endl;
			return ret;
		}
		
		// if bypassing middleman, check we have valid batch specs
		bool bypass_middleman=false;
		arg_store.Get("bypass_middleman",bypass_middleman);
		if(bypass_middleman){
			int batch_size=1, batch_size_min=1, batch_size_max=100;
			bool randomize_batchsize=false;
			arg_store.Get("batch_size",batch_size);
			arg_store.Get("batch_size_min",batch_size_min);
			arg_store.Get("batch_size_max",batch_size_max);
			arg_store.Get("randomize_batchsize",randomize_batchsize);
			if(batch_size_max<batch_size_min) std::swap(batch_size_min,batch_size_max);
			if( (!randomize_batchsize && batch_size<=0) ||
			    (randomize_batchsize && ((batch_size_min<=0 || batch_size_max<=0) || (batch_size_min==batch_size_max)))
			  ){
				std::string ret="no valid batch size specified";
				std::cerr<<ret<<std::endl;
				return ret;
			}
		}
		
		// -----------------------------------------------------------//
		// set initial thread state based on if waiting on slow control
		// -----------------------------------------------------------//
		m_log(v_message)<<"setting initial running state"<<std::endl;
		bool start_signal=false;
		arg_store.Get("start_signal",start_signal);
		if(start_signal){
			logger_args.running=false;
			std::clog<<"waiting for user to click 'Start'"<<std::endl;
		} else {
			std::clog<<"starting the loggers immediately on creation"<<std::endl;
			logger_args.running=true;
		}
		
		// --------------------------//
		// Kick off the sender threads
		// --------------------------//
		m_log(v_message)<<"creating worker threads"<<std::endl;
		int n_threads=1;
		arg_store.Get("n_threads",n_threads);
		
		for(int i=0; i<n_threads; ++i){
			logger_threads.emplace_back(std::async(std::launch::async, &LogSender, i, &logger_args)); /// no this as static?
			std::clog<<"created log sender "<<i<<std::endl;
		}
		
		// ------------- //
		// Set up cleanup
		// --------------//
		// the destructors of the futures returned by std::async spawning will block until the threads are done.
		// To return a propmt response to our SlowControl caller, we can defer this to a separate thread
		m_log(v_message)<<"creating reporter"<<std::endl;
		std::future<void> ret = std::async(std::launch::async, &SendManager::WaitLoggers, this);
		// just to avoid the compiler 'nodiscard' warning, capture the future and wait on it
		ret.wait();
		
		m_log(v_message)<<"done"<<std::endl;
		return "started "+std::to_string(n_threads)+" loggers";
		
	}
	
	// ==================================================================//
	// asynchronous helper function for thread wait and cleanup
	// ==================================================================//
	void WaitLoggers(){
		std::clog<<"waiting for "<<logger_threads.size()<<" loggers"<<std::endl;
		// strictly for synchronization we just need to invoke the destructors of the vector of futures
		// this will block until the associated thread is finished. But to calculate cumulative total
		// stats without worrying about synchronization, they get returned via the futures.
		std::valarray<int> tmp(0,5), totals(0,5); // msgs_sent, bytes_sent, msg_failures, runtime of thread
		int mintime=std::numeric_limits<int>::max(), maxtime=0, maxsend=0;
		for(std::future<std::valarray<int>>& res : logger_threads){
			tmp = res.get();
			totals += tmp;
			if(tmp[3]>maxtime) maxtime=tmp[3]; // runtime of slowest thread represents total runtime
			if(tmp[3]<mintime) mintime=tmp[3];
			if(tmp[4]>maxsend) maxsend=tmp[4]; // maximum time for any given query
		}
		std::clog<<"loggers finished"<<std::endl;
		std::stringstream resultstring;
		resultstring << "messages sent: "<<totals[0]<<" (including failed)"
		             <<"\nbytes sent: "<<totals[1]<<" (including failed)"
		             <<"\nfailed sends: "<<totals[2]
		             <<"\nnum threads: "<<logger_threads.size()
		             <<"\nlongest runtime: "<<maxtime
		             <<" ms\nshortest runtime: "<<mintime
		             <<" ms\naverage runtime: "<<(totals[3]/logger_threads.size())
		             <<" ms\nlongest query time: "<<maxsend<<"ms";
		DAQ_inter->sc_vars["Results"]->SetValue(resultstring.str());
		std::cout<<resultstring.str()<<std::endl;
		logger_threads.clear();
	}
	
	// ==================================================================//
	// Callback for signalled start of logger threads
	// ==================================================================//
	std::string StartLoggers(const char* payload){
		if(logger_threads.size()==0){
			std::string ret = "no loggers to start!";
			std::cerr<<ret<<std::endl;
			return ret;
		}
		if(logger_args.running==true){
			std::string ret="loggers already running!";
			std::cerr<<ret<<std::endl;
			return ret;
		}
		logger_args.running=true;
		std::string ret = "started loggers";
		std::clog<<ret<<std::endl;
		return ret;
	}
	
	// ==================================================================//
	// Callback for signalled stop of logger threads
	// ==================================================================//
	std::string StopLoggers(const char* payload){
		if(logger_threads.size()==0){
			std::string ret = "no loggers to stop!";
			std::cerr<<ret<<std::endl;
			return ret;
		}
		if(logger_args.running==false){
			std::string ret="loggers not running!";
			std::cerr<<ret<<std::endl;
			return ret;
		}
		logger_args.running=false;
		std::string ret = "stopped loggers";
		std::clog<<ret<<std::endl;
		return ret;
	}
	
	
	// ==================================================================//
	// The thread function to do actual sending
	// ==================================================================//
	static std::valarray<int> LogSender(int thread_id, MyThreadArgs* m_args){
		
		Store* arg_store = m_args->arg_store;
		DAQInterface* DAQ_inter = m_args->DAQ_inter;
		thread_local std::string thread_name = std::to_string(thread_id);
		if(DAQ_inter==nullptr){
			std::clog<<"thread "<<thread_name<<" has no DAQInterface!"<<std::endl;
			return std::valarray<int>(0,5);
		}
		
		int n_msgs=-1;                   // number of messages to send. Use -1 if determined from duration
		int duration_ms=-1;              // how long to send messages for. i.e. keep sending for N ms, then stop.
		int period_ms=-1;                // milliseconds to wait between message sends. Use -1 for no delay.
		bool randomize_period=false;     // whether to randomize time between messages.
		int period_ms_min=1;             // if randomizing, minimum delay.
		int period_ms_max=100;           // if randomizing, maximum delay.
		std::string msg="";              // message to send. if empty, randomly generated.
		int msg_length=100;              // how many characters in the log message, if randomly generated.
		bool randomize_msg=false;        // whether to generate a new random value for each log message.
		int msg_length_min=1;            // if every message is new, minimum message length. Set msg_length<0 to use this.
		int msg_length_max=100;          // if every message is new, maximum message length. Set msg_length<0 to use this.
		bool as_query=false;             // use API SQLQuery rather than SendLog
		bool bypass_middleman=false;     // bypass middleman (libDAQInterface API) and use pqxx to send to DB directly.
		// if bypassing middleman, we may do message batching.
		// (batching by libDAQInterface TODO but would be hidden from end user anyway)
		int batch_size=1;                // number of messages to batch up before sending.
		bool randomize_batchsize=false;  // whether to randomize number of messages in each batch.
		int batch_size_min=1;            // if randomizing, minimum batch size.
		int batch_size_max=100;          // if randomizing, maximum batch size.
		bool print_results=false;        // whether this thread should print its info
		
		// gettem all from Store
		arg_store->Get("n_msgs",n_msgs);
		arg_store->Get("duration_ms",duration_ms);
		arg_store->Get("period_ms",period_ms);
		arg_store->Get("randomize_period",randomize_period);
		arg_store->Get("period_ms_min",period_ms_min);
		arg_store->Get("period_ms_max",period_ms_max);
		arg_store->Get("msg",msg);
		arg_store->Get("msg_length",msg_length);
		arg_store->Get("randomize_msg",randomize_msg);
		arg_store->Get("msg_length_min",msg_length_min);
		arg_store->Get("msg_length_max",msg_length_max);
		arg_store->Get("as_query",as_query);
		arg_store->Get("bypass_middleman",bypass_middleman);
		arg_store->Get("batch_size",batch_size);
		arg_store->Get("randomize_batchsize",randomize_batchsize);
		arg_store->Get("batch_size_min",batch_size_min);
		arg_store->Get("batch_size_max",batch_size_max);
		arg_store->Get("print_results",print_results);
		//printf("thread %s will send %d messages\n",thread_name.c_str(),n_msgs);
		
		// be generous
		if(msg_length_max<msg_length_min) std::swap(msg_length_min,msg_length_max);
		if(period_ms_max<period_ms_min) std::swap(period_ms_min,period_ms_max);
		if(batch_size_max<batch_size_min) std::swap(batch_size_min,batch_size_max);
		
		// check if randomizing length as well as content of messages
		bool randomize_msglen = randomize_msg;
		if(randomize_msg && msg_length_min==msg_length_max){
			randomize_msglen = false;
			if(msg_length_min>0) msg_length = msg_length_min;
		}
		
		// if randomizing sleep times, ensure default is >0 (used as a check)
		if(randomize_period) period_ms=1;
		
		// helper function to generate a random in a range
		auto random = [](const int lower, const int upper) -> int {
			return lower + rand()%(upper-lower);
		};
		
		// helper to generate timestamps if bypassing middleman
		time_t rawtime;
		struct tm * timeinfo;
		char timestring[80];
		auto generate_timestring = [&rawtime, &timeinfo, &timestring]() -> void {
			time(&rawtime);
			timeinfo = localtime(&rawtime);
			strftime(timestring,80,"%F %T",timeinfo);
			return;
		};
		
		// helper function to generate random messages
		auto generate_msg = [&msg, &randomize_msglen, &msg_length, &msg_length_min, &msg_length_max, &bypass_middleman, &as_query, &random, &generate_timestring, &timestring/*, &thread_name*/]() -> void {
			if(randomize_msglen){
				msg_length = random(msg_length_min, msg_length_max);
			}
			msg.resize(msg_length);
			for(int i=0; i<msg_length; ++i){
				//msg[i] = random(32, 126); // readable ascii chars span 32-126
				// XXX actually if we bypass the middleman we may need to ensure we don't include apostrophes
				// FIXME actually even if we don't psql can't query the logging table, returnin 'invalid byte sequence for encoding "UTF8": 0x9a"
				msg[i] = random(97, 122); // lowercase letters only then, super safe
			}
			// if bypassing middleman or using SQLQuery we need to add the rest of the SQL values
			if(bypass_middleman || as_query){
				generate_timestring();
				msg = std::string("( '")
					    + timestring               + "','"
					    + thread_name              + "',"
					    + "0"                      + ",'"
					    + msg
					    + "' )";
			} // otherwise JSON wrapping and other fields are handled by libDAQInterface
			return;
		};
		
		// if bypassing middleman we also need an SQL prefix which is combined with the message(s) to form the pqxx query
		static const std::string prefix="INSERT INTO logging ( time, device, severity, message ) VALUES ";
		std::string query;
		
		// helper function to generate queries (assumes bypass_middleman)
		auto generate_query = [&query, &msg, &randomize_msg, &randomize_batchsize, &batch_size, &batch_size_min, &batch_size_max, &random, &generate_msg/*, &prefix*/]() -> void {
			query=prefix;
			if(randomize_batchsize){
				batch_size = random(batch_size_min, batch_size_max);
			}
			for(int i=0; i<batch_size; ++i){
				if(randomize_msg) generate_msg();
				if(i!=0) query += ",";
				query += msg;
			}
			query += ";";
		};
		
		// if not randomizing messages but we weren't given one, generate it now
		if(msg=="") generate_msg();
		
		// likewise if message & batch size is not varied between sends we can generate the SQL query now
		generate_query();
		
		// if bypassing middleman we also need to open a pqxx connection to the database
		pqxx::connection* conn=nullptr;
		if(bypass_middleman){
			std::string dbname="daq";
			std::string dbhost="localhost";
			std::string dbport="5432";
			std::string dbuser="root";
			arg_store->Get("dbname",dbname);
			arg_store->Get("dbhost",dbhost);
			arg_store->Get("dbport",dbport);
			arg_store->Get("dbuser",dbuser);
			
			std::stringstream tmp;
			if(dbname!="")   tmp<<" dbname="<<dbname;
			if(dbport!="")   tmp<<" port="<<dbport;
			if(dbuser!="")   tmp<<" user="<<dbuser;
			if(dbhost!="")   tmp<<" host="<<dbhost;
			
			try {
				conn = new pqxx::connection(tmp.str().c_str());
			} catch(std::exception& e){
				std::cerr<<"Thread "<<thread_name<<" exception "<<e.what()<<" opening connection to database!"<<std::endl;
				return std::valarray<int>(0,5);
			}
			if(!conn || !conn->is_open()){
				std::cerr<<"Thread "<<thread_name<<" failed opening connection to database!"<<std::endl;
				return std::valarray<int>(0,5);
			}
		}
		
		// track number of messages/bytes sent by this thread
		int msgs_sent=0;
		int bytes_sent=0;
		int msg_failures=0;
		int max_snd_ms=0;
		bool ok=true;
		std::string response; // API requires it
		
		// ok think that's everything ready
		// wait for start signal
		//if(m_args->running) std::clog<<"thread "<<thread_name<<" waiting for start signal"<<std::endl;
		while(!m_args->running){
			usleep(100);
		}
		//std::clog<<"thread "<<thread_name<<" starting"<<std::endl;
		
		std::chrono::time_point<std::chrono::high_resolution_clock> tot_start = std::chrono::high_resolution_clock::now();
		// loop until sending is done
		size_t loops=0;
		while(m_args->running){
			++loops;
			std::chrono::time_point<std::chrono::high_resolution_clock> snd_start = std::chrono::high_resolution_clock::now();
			// send next message (or batch)
			if(bypass_middleman){
				generate_query();
				pqxx::nontransaction txn(*conn);
				try {
					txn.exec(query);
				} catch(std::exception& e){
					std::cerr<<"thread "<<thread_name<<" exception "<<e.what()<<" sending message "<<msgs_sent<<std::endl;
					++msg_failures;
				}
				msgs_sent += batch_size;
				bytes_sent += query.length();
			} else {
				if(!as_query){
					if(randomize_msg) generate_msg();
					ok = DAQ_inter->SendLog(msg, 0, thread_name); // msg, severity, device
				} else {
					if(randomize_msg) generate_query();
					ok = DAQ_inter->SQLQuery(query,response);
				}
				
				// various other types depending on what we're benchmarking TODO
				//bool ok = DAQ_inter.SendAlarm(msg, 0, thread_name); // to test zmq Write speed XXX XXX NEEDS TIMEOUT XXX XXX disable multicast in backend!
				//bool ok = DAQ_inter.GetDeviceConfig(config_json, version, thread_name); // to test Read speed, XXX XXX NEEDS TIMEOUT NEED TO VARY RECORD XXX XXX
				// bool ok = DAQ_inter.SendDeviceConfig(config_json, "John Doe", "My New Config"); //for pre-filling...
				//bool ok = DAQ_inter.SQLQuery("daq",query,response, timeout); // Or maybe this is easier? response can be string or vector for num records
				
				if(!ok) ++msg_failures;
				++msgs_sent;
				bytes_sent += msg.length();
			}
			
			// keep track of max time a send took as this is important for clients
			// FIXME more stats?
			std::chrono::milliseconds snd_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - snd_start);
			if(snd_time.count()>max_snd_ms){
				max_snd_ms = snd_time.count();
			}
			
			// break if it's time to stop
			if(msgs_sent==n_msgs){ /*printf("all msgs sent, break\n");*/ break; }
			if(duration_ms>0){
				std::chrono::milliseconds tot_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - tot_start);
				if(tot_time.count()>duration_ms){ /*printf("duration met, break\n");*/ break; }
			}
			
			// sleep to maintain rate
			if(period_ms<0) continue;  // period<0 means "as fast as possible"
			if(randomize_period){
				period_ms = random(period_ms_min, period_ms_max);
			}
			int sleep_time=(period_ms - snd_time.count());
			if(sleep_time>0) usleep(sleep_time*1000);
			
		}
		//printf("loops: %d, running %d\n",loops,m_args->running);
		
		std::chrono::milliseconds tot_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - tot_start);
		
		if(bypass_middleman){
			if(conn) delete conn;
			conn=nullptr;
		}
		
		// pass them back for cumulative sum from all threads
		int runtime = tot_time.count();
		std::valarray<int> results{msgs_sent, bytes_sent, msg_failures, runtime, max_snd_ms};
		
		if(print_results){
			std::clog<<"thread "<<thread_name<<" sent "<<msgs_sent<<" messages in "<<loops<<" loops, totalling "<<bytes_sent
			         <<" bytes in "<<tot_time.count()<<"ms, of which "<<msg_failures<<" failed"<<std::endl;
		}
		
		return results;
	} // end of LogSender method
	
};  // end of SendManager class

//////////////////////////////////////////////////////////////////////////////
//                         main                                             //
//////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv){
	
	int verbose=1;
	
	std::string Interface_configfile = "./InterfaceConfig";
	
	if(verbose) std::clog<<"Constructing DAQInterface and waiting for middleman to connect"<<std::endl;
	DAQInterface DAQ_inter(Interface_configfile);
	std::string device_name = DAQ_inter.GetDeviceName(); //name of my device
	std::clog<<"DAQInterface should be connected! Sending test log"<<std::endl;
	std::this_thread::sleep_for(std::chrono::seconds(1));
	bool ok = DAQ_inter.SendLog("test logging", 0, device_name);
	if(!ok){
		std::cerr<<"Test log failed!"<<std::endl;
		return 1;
	}
	
	if(verbose) std::clog<<"Constructing SendManager"<<std::endl;
	SendManager send_mgr(&DAQ_inter, verbose);
	
	if(verbose) std::clog<<"Constructing slow controls"<<std::endl;
	DAQ_inter.sc_vars["Status"]->SetValue("Ready");
	
	DAQ_inter.sc_vars.Add("Run",BUTTON, std::bind(&SendManager::StartLogging, std::ref(send_mgr),  std::placeholders::_1));
	DAQ_inter.sc_vars["Run"]->SetValue(false);
	
	DAQ_inter.sc_vars.Add("Start",BUTTON, std::bind(&SendManager::StartLoggers, std::ref(send_mgr),  std::placeholders::_1));
	DAQ_inter.sc_vars["Start"]->SetValue(false);
	
	DAQ_inter.sc_vars.Add("Stop",BUTTON, std::bind(&SendManager::StopLoggers, std::ref(send_mgr),  std::placeholders::_1));
	DAQ_inter.sc_vars["Stop"]->SetValue(false);
	
	DAQ_inter.sc_vars.Add("Quit",BUTTON);
	DAQ_inter.sc_vars["Quit"]->SetValue(false);
	
	DAQ_inter.sc_vars.Add("Results",INFO);
	DAQ_inter.sc_vars["Results"]->SetValue(" No results yet...");
	
	// if given test settings file, run immediately
	if(argc>1){
		Store test_settings;
		test_settings.Initialise(argv[1]);
		if(verbose>3){
			std::clog<<"Configuration parameters are:\n------ "<<std::endl;
			test_settings.Print(); //print the current configuration
			std::clog<<"------"<<std::endl;
		}
		std::string settings_string;
		test_settings >> settings_string;
		send_mgr.StartLogging(settings_string.c_str());
		if(verbose) std::clog<<"Test complete"<<std::endl;
		return 0;
	}
	
	// otherwise wait for test specs via slow control and run them on request
	std::cout<<"Waiting for user to send 'Run' or 'Quit'..."<<std::endl;
	
	bool running=true;
	bool started=false;
	while(running){
		sleep(1);
		running=(!DAQ_inter.sc_vars["Quit"]->GetValue<bool>());
	}
	
	std::cout<<"Tests complete"<<std::endl;
	DAQ_inter.sc_vars["Status"]->SetValue("Finished");
	
	return 0;
	
}
