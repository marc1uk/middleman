#include "JsonParser.h"
#include <iostream>
#include <string>

int main(){
	JSONP parser;
	
	//parser.SetVerbose(1);
	std::string testjson="{ \"device\":\"mydevice\", \"signed\":-12, \"unsigned\":3, \"zero\":0, \"trailingspace\":  999 , "
                             "  \"unsigneds\":[10,11,12], \"signeds\":[-12,13,-15], \"floats\":[12.2,13.3,14.4], \"signeds2\":[10,-11,12], "
                             "\"floats2\":[10,-12,13.33],\"cat?\":\"yes\", \"inhomo\":[ 5, true, \"cat\" ], "
	                     "  \"nested\":[ { \"inner1\":\"1\", \"inner2\":\"2\" }, { \"inner3\":\"3\", \"inner4\":\"4\" } ], "
	                     " \"nested2\":[ [ 1,2,3 ], [4,5,6] ] }";
	
        // NOTE:
        // what specific type of integer is {"anum":5}? uint32_t? int64_t?
        // to prevent truncation we assume longs, but we need to use strol or stroul
        // depending on sign or unsigned to prevent possible invalid value (truncation or sign misinterpretation)
        // so we check numeric string for leading '-' and all negative integers will be int64_t, all positive integers uint64_t.
        // (for arrays, if any number is negative all are treated as signed, otherwise all unsigned.)
        // (all non-integral numbers will be double.)
        // However if typechecking is enabled this means you need to know if your numeric is + or -
        // to know the right type with which to Get it from the BStore, and that might be problematic if sign fluctuates.
        // So we turn typechecking off by default....
        // But then if we have JSON [ 5, true, null ], this goes into a BStore, and now the user needs to know
        // the types of every element in order to get them all appropriately...and that's a pain for a test script that
	// just wants to print them out.
        // so we use Evgenii's handy JsonEncode, but that requires typechecking on, so we turn it on just for testing.
	bool typechecking=false;
	BStore out(false,typechecking);
	parser.Parse(testjson, out);
	
	// notes as of time of writing (29-jan-2025):
	// BStore::Print(true) doesn't work as internals are binary and can't be printed
	// BStore::operator>> doesn't work as it needs updating to properly outupt valid JSON
	
	std::string device;
	//int64_t asigned;
	//uint64_t aunsigned;
	int32_t asigned;
	int32_t aunsigned;
	int64_t azero;
	int64_t trailing;
	std::vector<int64_t> signs;
	std::vector<int64_t> unsigns;
	std::vector<int64_t> signs2;
	std::vector<double> floats;
	std::vector<double> floats2;
	BStore arraystore(false,typechecking);
	BStore nestedobjs(false,typechecking);
	BStore nestedarrays(false,typechecking);
	
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
	ok = out.Get("inhomo",arraystore);
	ok = out.Get("nested",nestedobjs);
	ok = out.Get("nested2",nestedarrays);
	
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
	std::cout<<std::endl;
	
	std::cout<<"inhomo:"<<std::endl;
	int arraystoresize=0;
	ok = arraystore.Get("#",arraystoresize);
	std::cout<<"size: "<<arraystoresize<<std::endl;
	// bit of a stuck situation here though:
	// how do we know what the type of each element is?
	// JsonEncode to the rescue!
	for(int i=0; i<arraystoresize; ++i){
		std::string tmp;
		arraystore.JsonEncode(std::to_string(i),tmp);
		std::cout<<'\t'<<i<<": "<<tmp<<std::endl;
	}
	
	std::cout<<"nested:"<<std::endl;
	arraystoresize=0;
	nestedobjs.Get("#",arraystoresize);
	std::cout<<"size: "<<arraystoresize<<std::endl;
	for(int i=0; i<arraystoresize; ++i){
		BStore inner(false,true);
		nestedobjs.Get(std::to_string(i),inner);
		std::cout<<i<<":"<<std::endl;
		inner.Print(true);
	}


	std::cout<<"nested2:"<<std::endl;
	//nestedarrays.Print();
	arraystoresize=0;
	nestedarrays.Get("#",arraystoresize);
	std::cout<<"size: "<<arraystoresize<<std::endl;
	int elcount=0;
	for(int i=0; i<arraystoresize; ++i){
		
		std::vector<uint64_t> tmpvec;
		nestedarrays.Get(std::to_string(i),tmpvec);
		std::cout<<i<<": [";
		for(uint64_t& aval : tmpvec) std::cout<<aval<<"; ";
		std::cout<<"]"<<std::endl;
		
		/*
		BStore inner(false,true);
		nestedarrays.Get(std::to_string(i),inner);
		
		int innersize=0;
		inner.Get("#",innersize);
		std::cout<<"inner "<<i<<" of size "<<innersize<<std::endl;
		for(int j=0; j<innersize; ++j){
			std::string innerkey = std::string{"inner"}+std::to_string(elcount);
			innerval=0;
			inner.Get(innerkey,innerval);
			std::cout<<"\t\t"<<innerkey<<":"<<innerval<<std::endl;
			++elcount;
		}
		*/
	}
	
	std::cout<<"done"<<std::endl;
	
	return 0;
}

