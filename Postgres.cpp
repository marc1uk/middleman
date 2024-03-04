/* vim:set noexpandtab tabstop=4 wrap filetype=cpp */
#include "Postgres.h"
//#include <signal.h>
#include <unistd.h>   // fork
#include <stdlib.h>   // system
#include <sys/wait.h> // waitpid
#include <fstream>

void Postgres::SetVerbosity(int verb){
	verbosity=verb;
}

// TODO probably could do better with the exception handling throughout this file
pqxx::connection* Postgres::OpenConnection(std::string* err){
	if(verbosity>v_debug) std::cout<<"Opening Connection"<<std::endl;
	try{
		// if we already have a connection open, nothing to do
		if(conn!=nullptr && conn->is_open()){
			if(verbosity>v_debug) std::cout<<"Connection already open"<<std::endl;
			return conn;
		} else if(conn){
			// conn not null, but is not open...
			delete conn; // should we do this? e.g. lazy connections will open only on first use.
		}
		
		// otherwise form the connection string
		std::stringstream tmp;
		if(dbname!="")   tmp<<" dbname="<<dbname;
		if(port!=-1)     tmp<<" port="<<port;
		if(dbuser!="")   tmp<<" user="<<dbuser;
		if(dbpasswd!="") tmp<<" password="<<dbpasswd;
		if(hostaddr!="") tmp<<" hostaddr="<<hostaddr;
		if(hostname!="") tmp<<" host="<<hostname;
		
		// attempt to connect to the database
		if(verbosity>v_debug) std::cout<<"connecting with string '"<<tmp.str()<<"'"<<std::endl;
		conn = new pqxx::connection(tmp.str().c_str());
		
		// verify we succeeded
		// "don't use is_open(), use the broken_connection exception", they say. Hmm.
		// but will that be thrown now, or only when we try to *use* the connection, for a transaction?
		if(!conn->is_open()){
			std::cerr<<"Failed to connect to the database! Connection string was: '"
			         <<tmp.str()<<"', please verify connection details"<<std::endl;
			if(err) *err = "pqxx::connection::is_open() returned false after connection attempt";
			return nullptr;
		}
		return conn;
	}
	catch (const pqxx::broken_connection &e){
		// as usual the doxygen sucks, but it seems this doesn't provide
		// any further methods to obtain information about the failure mode,
		// so probably not useful to catch this explicitly.
		std::cerr << e.what() << std::endl;
		if(err) *err = e.what();
	}
	catch (std::exception const &e){
		std::cerr << e.what() << std::endl;
		if(err) *err = e.what();
	}
	return nullptr;
}

bool Postgres::CloseConnection(std::string* err){
	if(verbosity>v_debug){
		std::cout<<"Closing connection"<<std::endl;
	}
	if(conn==nullptr){
		if(verbosity>v_debug) std::cout<<"No connection to close"<<std::endl;
		return true;
	}
	try {
		if(!conn->is_open()){
			if(verbosity>v_debug) std::cout<<"Connection is not open"<<std::endl;
			return true;
		}
		conn->disconnect();
		if(conn->is_open()){
			std::cerr<<"Attempted to close postgresql connection, yet it remains open?!" <<std::endl;
			if(err) *err="pqxx::connection::is_open() returns true even after calling disconnect";
			return false;
		}
		return true;
	}
	catch (const pqxx::broken_connection &e){
		return true; // sure
	}
	catch (std::exception const &e){
		std::cerr << e.what() << std::endl;
		if(err) *err=e.what();
		return false; // umm....
	}
	return false; // dummy
}

Postgres::~Postgres(){
	CloseConnection();
	if(conn) delete conn;
}

Postgres::Postgres(){}

void Postgres::Init(std::string hostname_in, std::string hostip_in, int port_in,
               std::string user_in, std::string password_in, std::string dbname_in){
	// apparently when a connection breaks, it not only throws an exception
	// but sends a SIGPIPE signal that by default will kill the application!
	// https://libpqxx.readthedocs.io/en/6.3/a00915.html
	// let's please not do that.
	signal(SIGPIPE, SIG_IGN);
	// set connection details
	hostname=hostname_in;
	hostaddr=hostip_in;
	port=port_in;
	dbuser=user_in;
	dbname=dbname_in;
	dbpasswd=password_in;
}

// XXX reminder that pqxx::result is a reference-counting wrapper and is not thread-safe! XXX
bool Postgres::Query(std::string query, int nret, pqxx::result* res, pqxx::row* row, std::string* err){
	// maybe this is redundant since OpenConnection will check is_open (against recommendations)
	for(int tries=0; tries<2; ++tries){
		// ensure we have a connection to work with
		if(OpenConnection(err)==nullptr){
			// no connection to batabase -> abort
			return false;
		}
		try{
			// open a transaction to interact with the database
			//pqxx::work(*conn);
			pqxx::nontransaction txn(*conn);
			
			// run the requested query
			// the type of exec we run is based on the user's expected number of returned rows, nret
			if(nret==0){
				txn.exec0(query);
				return true;
			} else if(res==nullptr && row==nullptr){
				std::string msg =  "Postgres::ExecuteQuery called with expected number of returned rows ";
				            msg += std::to_string(nret)+" but nowhere to return the result!";
				std::cerr<<msg<<std::endl;
				if(err) *err=msg;
				// we'll run the query anyway, just in case the user has some reason to invoke
				// a query with a return that they don't actually want, i guess....?
			} else if(nret>1 && res==nullptr){
				std::string msg =  "Postgres::ExecuteQuery called with expected number of returned rows ";
				            msg += std::to_string(nret)+" but only given a return pointer for one row!";
				            msg += "Only the first row will be returned";
				std::cerr<<msg<<std::endl;
				if(err) *err=msg;
				// again we could be harsh and forbid this, which may help flag bugs, but we'll proceed...
			}
			if(nret==1 && res==nullptr && row!=nullptr){
				// user expects one returned row, and only wants the returned row. perfect.
				*row = txn.exec1(query);
			} else {
				// else either the user expects more than one row, or they want the pqxx::result,
				// so use a general exec
				if(res!=nullptr){
					*res = txn.exec(query);
					if(row!=nullptr){
						// i guess they want us to extract the first row too...?
						*row = (*res)[0];
					}
				} else {
					// they've only given us a row, but have told us they expect more than one row...
					pqxx::result loc_res = txn.exec(query);
					*row = loc_res[0];
				}
			}
			// if no exceptions thrown, we're done.
			return true;
		}
		catch (const pqxx::broken_connection &e){
			// if our connection is broken after all, disconnect, reconnect and retry
			if(tries==0){
				CloseConnection();
				delete conn; conn=nullptr;
				continue;
			} else {
				std::cerr<<"Postgres::Query error - broken connection, failed to re-establish it"<<std::endl;
				if(err) *err=e.what();
			}
		}
		catch (const pqxx::sql_error &e){
			std::string msg = e.what();
			            msg += "When executing query: " + e.query();
			if(e.sqlstate()!=""){
				msg += ", with SQLSTATE error code: " + e.sqlstate();
			}
			std::cerr<<msg<<std::endl;
			if(err) *err = msg;
			// from the discussion on the transactor framework page
			// (https://libpqxx.readthedocs.io/en/6.3/a00258.html)
			// it seems transactions can fail for transient reasons.
			// if for some reason we're not using the transactor framework
			// but still want to retry the query manually, do that here.
			// continue;    // along with any other necessary reinitializations and whatnot
		}
		catch (std::exception const &e){
			std::cerr << e.what() << std::endl;
			if(err) *err = e.what();
		}
		break;   // if not explicitly 'continued', break.
	}
	// if we haven't returned true, something went wrong.
	return false;
}

bool Postgres::QueryAsStrings(std::string query, std::vector<std::string> *results, char row_or_col, std::string* err){
	// generically run a query, without knowing how many returns are expected.
	// we'll need to get the results in a generic pqxx::result, and specify the number
	// of returned rows is >1. If there's fewer, it'll just return an empty container.
	pqxx::result res;
	get_ok = Query(query, 2, &res, nullptr, err);
	// if the query failed, the user didn't provide means for a return, or the query had no return,
	// then we have no need to parse the response and we're done.
	if(not get_ok || results==nullptr || res.size()==0) return get_ok;
	// otherwise, parse the response
	// we're given a vector for putting results in.
	// this may be several fields of one row, or one field from several rows
	if(row_or_col=='r'){
		// row mode: user is querying many fields from one row
		pqxx::row row = res[0];  // XXX we discard any notice of additional rows...
		// Iterate over fields
		for (const pqxx::field field : row){
			results->push_back(field.c_str());
		}
	} else {
		// column mode: one column from many rows
		for(const pqxx::row row : res){
			pqxx::field field = row[0];  // XXX we discard any notice of additional fields...
			results->push_back(field.c_str());
		}
	}
	return true;
}

bool Postgres::QueryAsJsons(std::string query, std::vector<std::string> *results, std::string* err){
	// generically run a query, without knowing how many returns are expected.
	// we'll need to get the results in a generic pqxx::result, and specify the number
	// of returned rows is >1. If there's fewer, it'll just return an empty container.
	//printf("QueryAsJsons running '%s'\n",query.c_str());
	pqxx::result res;
	get_ok = Query(query, 2, &res, nullptr, err);
	// if the query failed, the user didn't provide means for a return, or the query had no return,
	// then we have no need to parse the response and we're done.
	if(not get_ok || results==nullptr || res.size()==0) return get_ok;
	// otherwise, parse the response. iterate over returned rows
	for(pqxx::row row : res){
		// build a json from fields in this row
		std::stringstream tmp;
		tmp << "{";
		for (pqxx::row::iterator it=row.begin(); it<row.end(); ){
			tmp << "\"" << it->name() << "\":\""<< it->c_str() << "\"";
			++it;
			if(it!=row.end()) tmp << ", ";
		}
		tmp << "}";
		results->push_back(tmp.str());
	}
	return true;
}

bool Postgres::Promote(int wait_seconds, std::string* err){
	std::string promote_query = "pg_promote(TRUE,"+std::to_string(wait_seconds)+")";
	// TRUE says to wait; otherwise we don't know whether the promotion succeeded.
	// wait is the time we wait for the promotion to succeed before aborting.
	return Query(promote_query, 0, nullptr, nullptr, err);
}

bool Postgres::Demote(int wait_seconds, std::string* err){
	/* FIXME implement using repmgr or pgbackrest */
	// as a minimal start, we can just make the standby.signal file and issue `pg_ctl restart`
	// however, if there are inconsistencies in this instance and the new master, startup may fail!
	if(err) *err = "";  // clear error for return.
	
//	// get db name
//	std::string query_string = "SELECT current_database()";
//	std::string dbname;
//	get_ok = ExecuteQuery(query_string, dbname);
//	if(not get_ok){
//		std::string msg = "Failed to get name of current database in Postgres::Demote!";
//		std::cerr<<msg<<std::endl;
//		if(err) *err=msg;
//		return false;
//	}
	
	// and db directory
	std::string query_string = "SELECT setting FROM pg_settings WHERE name='data_directory'";
	std::string dbdir;
	get_ok = ExecuteQuery(query_string, dbdir);
	
	// make the signal file that the db should start up in standby mode
	std::string flagfilepath = dbdir + "/standby.signal";
	std::cout<<"database stopped. Creating standby flag file "<<flagfilepath<<std::endl;
	std::ofstream flagfile(flagfilepath.c_str(),std::ios::out);
	// double check we succeeded
	if(flagfile.is_open()){
		flagfile.close();
	} else {
		std::string errmsg= "Failed to create standby flag file "+flagfilepath +"! Database left down!";
		std::cerr<<errmsg<<std::endl;
		if(err) *err = errmsg;
		return false;
	}
	
	// run the pg_ctl command. We can't do this via a query, it needs a system call.
	// it would probably be sufficient to use 'system(...)' here...
	// fork this process
	pid_t pid = fork();
	if (pid == -1) {
		// error in the fork attempt. This will have set the value of the global 'errno'
		// use 'strerror' to get a description of the error indicated by errno
		std::string errmsg(strerror(errno));
		errmsg = "Postgres::Demote failed to fork! Error: " + errmsg;
		std::cout<<errmsg<<std::endl;
		if(err) *err=errmsg;
		return false;
	}
	// this results in two processes that will both execute the following code.
	// Each fork receives a different value for the returned pid_t - we need to generate
	// the appropriate action for each fork based on the corresponding value.
	int status;
	if(pid==0){
		// child fork.
		// make a system call to stop the postgres database.
		std::cout<<"restarting database in standby mode"<<std::endl;
		// execl is a variadic function, first is executable, remainder are its arguments,
		// (remembering that argv[0] is conventionally just the name of the executable)
		// the list must be terminated by a 'const char*' typecast null ptr.
		std::string wait_string = std::to_string(wait_seconds);
		execl("pg_ctl", "pg_ctl", "-D", dbdir.c_str(), "-t", wait_string.c_str(), "restart", (char*)0);
		// here's a really bizarre thing - if an exec* call succeeds, it never returns...
		// for this reason we can't really chain a series of commands in one fork.
		// ✋ ...just ... leavin' me hanging here, bro...
		// it will, however, return if it fails.
		// Well, well, well... look who came crawling back...
		std::string errmsg(strerror(errno));
		errmsg = "Postgres::Demote execl call failed! Error: " + errmsg;
		std::cerr<<errmsg<<std::endl;
		if(err) *err = errmsg;
		return false;
	} else {
		// parent fork. 'pid' is set to the process id of the child.
		// we can use this to wait for the child to terminate.
		int status;
		waitpid(pid, &status, 0);
	}
	
	// check the termination status of the pg_ctl call
	bool succeeded = false;
	if(WIFEXITED(status)){
		// returned "normally" - check the return value
		int retval = WEXITSTATUS(status);
		// if it didn't complete in the requested timeout, pg_ctl will return a non-zero value
		if(retval!=0){
			std::string errmsg = "failed to restart database for demotion! pg_ctl restart failed!";
			std::cerr<<errmsg<<std::endl;
			if(err) *err = errmsg;
		} else {
			// seems to have succeeded.
			succeeded = true;
		}
	} else if(WIFSIGNALED(status)){
		// returned due to receipt of a signal
		std::string sigstring(strsignal(WTERMSIG(status)));
		std::string errmsg = "pg_ctl stop interrupted by signal " + sigstring +" during demotion!";
		std::cerr<<errmsg<<std::endl;
		if(err) *err = errmsg;
	} // or many others, see http://man.yolinux.com/cgi-bin/man2html?cgi_command=waitpid(2)
	
	// since some of these are a little ambiguous we should double check the server status explicitly
	// let's do a sanity check to see if we can query the database, and if we're now in recovery mode
	bool in_recovery=false;
	get_ok = ExecuteQuery("SELECT pg_is_in_recovery()",in_recovery);
	if(get_ok && in_recovery){
		// all looks good!
		std::cout<<"Database restarted in standby mode"<<std::endl;
		succeeded = true;
	} else {
		// something's not right.
		if(!get_ok){
			// we can't run a query. Database is down!
			std::string errmsg = "Failed to verify standby status - query failed.";
			std::cerr<<errmsg<<std::endl;
			if(err) *err = *err + " " + errmsg;
			// TODO verify server status with `pg_ctl status`?
			succeeded = false;
		} else {
			// database reported it is not in recovery mode? Did it failed to find standby.signal?
			std::string errmsg = "Database not in recovery mode after demotion!";
			std::cerr<<errmsg<<std::endl;
			if(err) *err = *err + " " + errmsg;
			succeeded = false;
		}
	}
	
	/*
	if(not succeeded){
		// We could have an issue that the master and standby are out of sync and both have changes.
		// in order to re-synchronize them we need to call pg_rewind on the standby,
		// which will rewind it to a point consistent with the master.
		// doing so will, however, discard all standby database transactions that aren't held
		// by the master. 
		// But we don't want to just lose these! We should dump them (which transactions?)
		// and then try to re-apply them afterwards (is that safe? Can we detect conflicts?)
		// TODO
		system("pg_dumpall");
		system("pg_rewind");
		// if still not ok, we need manual intervention.
	}
	*/
	
	return succeeded;
}

// Quoting functions
// =================
// quote field or table names (nominally use double quotes)
bool Postgres::pqxx_quote_name(const std::string& in, std::string& out, std::string* err){
	out = in; // often this will be sufficient
	if(OpenConnection(err)==nullptr) return false;
	try {
		out = conn->quote_name(in);
		return true;
	} catch (std::exception& e){
		std::string errmsg = std::string("Postgres::pqxx_quote_name threw exception ")
		                     +e.what()+" trying to quote '"+in+"'";
		std::cerr<<errmsg<<std::endl;
		if(err) *err=errmsg;
	}
	return false;
}

// quote values (nominally, use single quotes)
// TODO error signalling here sucks; shouldn't just return empty on failure!
bool Postgres::pqxx_quote(const std::string& in, std::string& out, std::string* err){
	out = in;
	//return pqxx::nullconnection{}.quote(string);
	// annoyingly we can't use a null connection just to get libpqxx to quote things for us;
	// we must have a valid connection to a real database :/
	if(OpenConnection(err)==nullptr) return false;
	try {
		out = conn->quote(in);
		return true;
	} catch (std::exception& e){
		std::string errmsg = std::string("Postgres::pqxx_quote<std::string> threw exception ")
		                     +e.what()+" trying to quote '"+in+"'";
		std::cerr<<errmsg<<std::endl;
		if(err) *err=errmsg;
	}
	return false;
}
