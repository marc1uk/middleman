#include "ReceiveSQL.h"
#include <iostream>
#include "unistd.h"

int main(int argc, const char* argv[]){
	
	if(argc==1){
		std::cout<<"Usage: "<<argv[0]<<" <configfile>"<<std::endl;
		return 0;
	}
	std::cout<<"Making ReceiveSQL"<<std::endl;
	ReceiveSQL receiver;
	
	std::cout<<"Initialising"<<std::endl;
	std::string configfile = argv[1];
	bool get_ok = receiver.Initialise(configfile);
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
		std::cout<<"Program will terminate when the stopfile is found"<<std::endl;
		return -1;
	}
	std::ifstream test(stop_file.c_str());
	if(test.is_open()){
		std::string cmd = "rm "+stop_file;
		system(cmd.c_str());
	}
	
	std::cout<<"Beginning loop"<<std::endl;
	while(true){
		//std::cout<<">>>>>> loop <<<<<<<<"<<std::endl;
		try{
			receiver.Execute();
		} catch(std::exception& e){
			std::cerr<<"CAUGHT EXCEPTION: "<<e.what()<<" for ReceiveSQL::Execute call in middleman!"<<std::endl;
		}
		
		//std::cout<<"checking for stopfile"<<std::endl;
		std::ifstream stopfile(stop_file);
		if (stopfile.is_open()){
			std::cout<<"Stopfile found, terminating"<<std::endl;
			stopfile.close();
			std::string cmd = "rm "+stop_file;
			system(cmd.c_str());
			break;
		}
		
	}
	
	std::cout<<"Finalising"<<std::endl;
	receiver.Finalise();
	
	std::cout<<"Done, exiting"<<std::endl;
	
	return 0;
}
