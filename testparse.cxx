#include "JsonParser.h"
#include <iostream>
#include <string>

int main(){
	JSONP parser;
	//parser.SetVerbose(1);
	std::string testjson="{ \"device\":\"mydevice\", \"signed\":-12, \"unsigned\":3, \"zero\":0, \"trailingspace\":  999 , "
                             "  \"unsigneds\":[10,11,12], \"signeds\":[-12,13,-15], \"floats\":[12.2,13.3,14.4], \"signeds2\":[10,-11,12], "
                             "\"floats2\":[10,-12,13.33],\"cat?\":\"yes\" }";
	BStore out(false,false);
	parser.Parse(testjson, out);
	
	// notes as of time of writing (29-jan-2025):
	// BStore::Print(true) doesn't work as internals are binary and can't be printed
	// BStore::operator>> doesn't work as it needs updating to properly outupt valid JSON
	
	std::string device;
	int64_t asigned;
	uint64_t aunsigned;
	int64_t azero;
	int64_t trailing;
	std::vector<int64_t> signs;
	std::vector<int64_t> unsigns;
	std::vector<int64_t> signs2;
	std::vector<double> floats;
	std::vector<double> floats2;
	
	bool ok;
	ok = out.Get("device",device);
	ok = out.Get("signed",asigned);
	ok = out.Get("unsigned",aunsigned);
	ok = out.Get("zero",azero);
	ok = out.Get("trailingspace",trailing);
	out.Get("signeds",signs);
	out.Get("unsigneds",unsigns);
	out.Get("signeds2",signs2);
	out.Get("floats",floats);
	out.Get("floats2",floats2);
	
	std::cout<<"device: "<<device
		<<", signed: "<<asigned
		<<", unsigned: "<<aunsigned
		<<", zero: "<<azero
		<<", trailingspace: "<<trailing
		<<std::endl;
	std::cout<<"signeds:"<<std::endl;
	for(auto& asigned : signs){
		std::cout<<asigned<<", ";
	}
	std::cout<<std::endl;
	std::cout<<"unsigneds:"<<std::endl;
	for(auto& ausigned : unsigns){
		std::cout<<ausigned<<", ";
	}
	std::cout<<std::endl;
	std::cout<<"floats:"<<std::endl;
	for(auto& afloat : floats){
		std::cout<<afloat<<", ";
	}
	std::cout<<std::endl;
	std::cout<<"signeds2:"<<std::endl;
	for(auto& asigned : signs2){
		std::cout<<asigned<<", ";
	}
	std::cout<<std::endl;
	std::cout<<"floats2:"<<std::endl;
	for(auto& afloat : floats2){
		std::cout<<afloat<<", ";
	}
	
	
	
	return 0;
}

