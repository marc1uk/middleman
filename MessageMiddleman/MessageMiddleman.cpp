#include <iostream>
#include <locale>
#include <DAQInterface.h>

using namespace ToolFramework;

int main(int argc, const char* argv[]){
	
	if(argc<3){
		std::cout<<"usage: "<<argv[0]<<" [type] [message]"<<std::endl;
		std::cout<<"[type]: 0=multicast, 1=tcp"<<std::endl;
		return 0;
	}
	
	std::string topic = argv[1];
	std::string msg="";
	for(int i=2; i<argc; ++i){
		msg += std::string(argv[i]);
	}
	for(char& c : topic){
		c=std::tolower(c);
	}
	
	std::string Interface_configfile = "./MiddlemanTesterConfig";
	std::cout<<"Constructing DAQInterface (will incur a wait to allow middleman to connect)..."<<std::endl;
	DAQInterface DAQ_inter(Interface_configfile);
	std::cout<<"Constructed"<<std::endl;
	DAQ_inter.sc_vars["Status"]->SetValue("Initialising"); //setting status message
	
	if(topic=="log"){
		// send logging multicast message
		std::cout<<"logging '"<<msg<<"'"<<std::endl;
		bool ok = DAQ_inter.SendLog(msg);
		std::cout<<"ok: "<<ok<<std::endl;
	} else if(topic=="monitor"){
		// send monitoring multicast message
		std::cout<<"monitoring '"<<msg<<"'"<<std::endl;
		bool ok = DAQ_inter.SendMonitoringData(msg);
		std::cout<<"ok: "<<ok<<std::endl;
	} else if(topic=="alarm"){
		// send tcp message
		std::cout<<"sending alarm '"<<msg<<"'"<<std::endl;
		bool ok = DAQ_inter.SendAlarm(msg);
		std::cout<<"ok: "<<ok<<std::endl;
	} else if(topic=="query"){
		std::string resp;
		std::cout<<"sending query '"<<msg<<"'"<<std::endl;
		bool ok = DAQ_inter.SQLQuery("daq",msg,resp, 1000);
		std::cout<<"ok: "<<ok<<", resp: "<<resp<<std::endl;
	} else if(topic=="rconfig"){
		std::string resp;
		std::cout<<"requesting config for device "<<msg<<std::endl;
		bool ok = DAQ_inter.GetDeviceConfig(resp, -1, msg);
		std::cout<<"ok: "<<ok<<", resp: "<<resp<<std::endl;
	} else {
		std::cerr<<"unknown topic"<<std::endl;
	}
	
	return 0;
	
}
