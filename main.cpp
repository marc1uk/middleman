#include "DemoClient.h"
#include <iostream>
#include "unistd.h"
#include <thread>
#include <chrono>

int main(int argc, const char* argv[]){
	
	if(argc==1){
		std::cout<<"Usage: "<<argv[0]<<" <configfile>"<<std::endl;
		return 0;
	}
	std::cout<<"Making DemoClient"<<std::endl;
	DemoClient sender;
	
	std::cout<<"Initialising"<<std::endl;
	std::string configfile = argv[1];
	bool get_ok = sender.Initialise(configfile);
	if(not get_ok){
		std::cerr<<"Failed to initialize!"<<std::endl;
		return -1;
	}
	if(argc>2){
		int nsecs = atoi(argv[2]);
		std::cout<<"sleeping for "<<nsecs<<"s to allow service discovery to find us"<<std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(nsecs));
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
		sender.Execute();
		
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
	sender.Finalise();
	
	std::cout<<"Done, exiting"<<std::endl;
	
	return 0;
}
