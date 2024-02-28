#include "ReceiveSQL.h"
#include <iostream>
#include "unistd.h"
#include <thread>
#include <chrono>

int main(int argc, const char* argv[]){
	
	if(argc==1){
		std::cout<<"Usage: "<<argv[0]<<" <configfile>"<<std::endl;
		return 0;
	}
	std::cout<<"Making ReceiveSQL"<<std::endl;
	ReceiveSQL* receiver = new ReceiveSQL{};
	
	std::cout<<"Initialising"<<std::endl;
	std::string configfile = argv[1];
	bool get_ok = true;
	try {
		get_ok = receiver->Initialise(configfile);
	} catch(std::exception& e){
		std::cerr<<"CAUGHT EXCEPTION: "<<e.what()<<" from ReceiveSQL::Initialise in middleman!"<<std::endl;
		get_ok=false;
	} catch(...){
		std::cerr<<"ReceiveSQL::Initialise uncaught error "<<errno<<std::endl;
		get_ok=false;
	}
	if(not get_ok){
		std::cerr<<"Failed to initialize!"<<std::endl;
		return -1;
	}
	Store configs;
	configs.Initialise(configfile);
	std::string stop_file;
	get_ok = configs.Get("stopfile",stop_file);
	if(not get_ok){
		std::cout<<"Please include 'stopfile' in configuration"<<std::endl;
		std::cout<<"Program will restart when the stopfile is found"<<std::endl;
		return -1;
	}
	std::ifstream test(stop_file.c_str());
	if(test.is_open()){
		test.close();
		std::string cmd = "rm "+stop_file;
		system(cmd.c_str());
	}
	
	// quitfile is relevant when calling this application with `run_middleman.sh` wrapper
	// which implements automatic re-starting should the middleman crash
	// it makes sense that the middleman will stop when the quit file is created as well,
	// but this is not strictly necessary if not using `run_middleman.sh`
	std::string quit_file="";
	get_ok = configs.Get("quitfile",quit_file);
	test.open(quit_file.c_str());
	if(quit_file!="" && test.is_open()){
		test.close();
		std::string cmd = "rm "+quit_file;
		system(cmd.c_str());
	}
	
	std::cout<<"Beginning loop"<<std::endl;
	while(true){
		//std::cout<<">>>>>> loop <<<<<<<<"<<std::endl;
		try{
			receiver->Execute();
		} catch(std::exception& e){
			std::cerr<<"CAUGHT EXCEPTION: "<<e.what()<<" from ReceiveSQL::Execute in middleman!"<<std::endl;
		} catch(...){
			std::cerr<<"ReceiveSQL::Execute uncaught error "<<errno<<std::endl;
		}
		
		//std::cout<<"checking for stopfile"<<std::endl;
		bool stopping=false;
		if(quit_file!="") test.open(quit_file);
		if(test.is_open()){
			test.close();
			stopping=true;
		}
		test.open(stop_file);
		if(test.is_open()){
			test.close();
			stopping=true;
			std::string cmd = "rm "+stop_file;
			system(cmd.c_str());
		}
		if(stopping){
			std::cout<<"Stopfile found, terminating"<<std::endl;
			break;
		}
		
		// DEBUG: limit rate of checks
		//std::this_thread::sleep_for(std::chrono::milliseconds(1000));
	}
	
	std::cout<<"Finalising"<<std::endl;
	try {
		receiver->Finalise();
	} catch(std::exception& e){
		std::cerr<<"CAUGHT EXCEPTION: "<<e.what()<<" from ReceiveSQL::Finalise in middleman!"<<std::endl;
	} catch (...){
		std::cerr<<"Finalise uncaught error "<<errno<<std::endl;
	}
	
	std::cout<<"Destroying receiver"<<std::endl;
	try {
		delete receiver;
	} catch(std::exception& e){
		std::cerr<<"CAUGHT EXCEPTION: "<<e.what()<<" from ReceiveSQL::~ReceiveSQL in middleman!"<<std::endl;
	} catch (...){
		std::cerr<<"ReceiveSQL destructor uncaught error "<<errno<<std::endl;
	}
	
	std::cout<<"Done, exiting"<<std::endl;
	
	return 0;
}
