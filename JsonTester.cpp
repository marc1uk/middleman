#include "JsonParser.h"
#include "BoostStore.h"
#include <iostream>

int main(int argc, const char** argv){
//	std::string testjson="{\"not true\": [0, false], \"true\": true, \"not null\": [0, 1, false, true, {\"obj\": null}, \"a string\"] }";
//	std::string testjson="{}";
//	std::string testjson="[0]";
//	std::string testjson="{\"Sally\": \"Father McKenzie, I thought you were DEAD!\", \"McKenzie\": \"I vas!\"}";
//	std::string testjson="[{   \"why\":null} ]";
//	std::string testjson="{ \"xvals\":[\"2022-09-23 02:04:55\",\"2022-09-23 01:48:08\",\"2022-09-23 01:31:45\"], \"yvals\":[0.497962,0.502168,0.498470] }";
//	std::string testjson="{\"purefit\":1, \"fits\":[ {\"method\":\"raw\", \"absfit\":1, \"peakdiff\":0.135514, \"gdconc\":0.055772 }, {\"method\":\"simple\", \"absfit\":1, \"peakdiff\":0.118749, \"gdconc\":0.055036 }, {\"method\":\"complex\", \"absfit\":1, \"peakdiff\":0.120748, \"gdconc\":0.055113 } ] }";
//	std::string testjson="{\"runnum\":128,\"start\":\"2022-09-09T00:21:09.356056\",\"stop\":null,\"runconfig\":1,\"notes\":\"\nfirst in-situ run with database, marcusanalysis, transparency and web page updating.\",\"git_tag\":\"V2.1.2-76-g2b4c532\",  \"output_file\": \"05SeptEGADS\" }";
//	std::string testjson="[ 1, 2, 3.4 ]";  // returns std::vector<double>
// all the following convert to std::vector<BoostStore>
//	std::string testjson="[ 1, true ]";
//	std::string testjson="[ 1, 1.1, true ]";
//	std::string testjson="[ 1, 2.2, \"three\"]";
//	std::string testjson="[ 1, null ]";
//	std::string testjson="[ 1, true, null ]";
//	std::string testjson="[ 1, true, null, \"potato\" ]";
//	std::string testjson="[ [1,2,3], [4.4,5.5,6.6] ]";
//	std::string testjson="{\"VME\": 1, \"PGTool\": 2, \"SlackBot\": 3, \"RunControl\": 4}";
	
//	// adapted from https://github.com/briandfoy/json-acceptance-tests, pass1
	std::string testjson=
		"{\n"
//		"\"integer\": 1234567890,\n"
//		"\"real\": -9876.543210,\n"
//		"\"e\": 0.123456789e-12,\n"
//		"\"E\": 1.234567890E+34,\n"
//		"\"\":  23456789012E66,\n"
//		"\"zero\": 0,\n"
//		"\"one\": 1,\n"
//		"\"space\": \" \",\n"
//		"\"quote\": \"\\\"\",\n"
//		"\"backslash\": \"\\\\\",\n"
//		"\"controls\": \"\\b\\f\\n\\r\\t\",\n"
//		"\"slash\": \"/ & \\/\",\n"
//		"\"alpha\": \"abcdefghijklmnopqrstuvwyz\",\n"
//		"\"no value\":,\n"
//		"\"ALPHA\": \"ABCDEFGHIJKLMNOPQRSTUVWYZ\",\n"
//		"\"digit\": \"0123456789\",\n"
//		"\"0123456789\": \"digit\",\n"
//		"\"special\": \"`1~!@#$%^&*()_+-={':[,]}|;.</>?\",\n"
//		"\"hex\": \"\\u0123\\u4567\\u89AB\\uCDEF\\uabcd\\uef4A\",\n"
//		"\"true\": true,\n"
//		"\"false\": false,\n"
//		"\"null\": null,\n"
//		"\"array\":[  ],\n"
//		"\"object\":{  },\n"
		"\"address\": \"50 St. James Street\",\n"
//		"\"url\": \"http://www.JSON.org/\",\n"
		"\"comment\": \"// /* <!-- --\",\n"     // XXX for just this, BStore.Print(1) shows the value but not the key???
//		"\"# -- --> */\": \" \",\n"
//		"\" s p a c e d \" :[1,2 , 3\n"
//		"\n"
//		",\n"
//		"\n"
//		"4 , 5        ,          6           ,7        ],\"compact\":[1,2,3,4,5,6,7,],\n"
//		"\"jsontext\": \"{\\\"object with 1 member\\\":[\\\"array with 1 element\\\"]}\",\n"
//		"\"quotes\": \"&#34; \\u0022 %22 0x22 034 &#x22;\",\n"
//		"\"\\/\\\\\\\"\\uCAFE\\uBABE\\uAB98\\uFCDE\\ubcda\\uef4A\\b\\f\\n\\r\\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?\"\n"
//		": \"A key can be any string\"\n"
		"}";
	
	BStore store{};
	JSONP parser{};
//	parser.SetVerbose(true);
	bool ok = parser.Parse(testjson, store);
	std::cout<<"parser ok? "<<ok<<std::endl;
	std::cout<<"Store contents:\n";
	std::cout<<"================"<<std::endl;
	if(argc>1 && std::string(argv[1])=="1") store.Print(true);
	else store.Print(false);
	std::cout<<"================"<<std::endl;
	
	bool boolean;
	int64_t a_numeric;
	double d_numeric;
	std::string a_str;
	std::vector<int64_t> v;
	BStore tmpstore;
//	ok=store.Get("true",boolean);
//	if(ok){
//		std::cout<<"true => "<<boolean<<std::endl;
//	}
//	ok=store.Get("false",boolean);
//	if(ok){
//		std::cout<<"false => "<<boolean<<std::endl;
//	}
//	ok=store.Get("null",a_str);
//	if(ok){
//		std::cout<<"null => '"<<a_str<<"'"<<std::endl;
//	}
//	store.Get("integer", a_numeric);
//	std::cout<<"integer => "<<a_numeric<<std::endl;
//	store.Get("real", d_numeric);
//	std::cout<<"real => "<<d_numeric<<std::endl;
//	store.Get("zero", a_numeric);
//	std::cout<<"zero => "<<a_numeric<<std::endl;
//	store.Get("one", a_numeric);
//	std::cout<<"one => "<<a_numeric<<std::endl;
//	store.Get("e", d_numeric);
//	std::cout<<"e => "<<d_numeric<<std::endl;
//	store.Get("E", d_numeric);
//	std::cout<<"E => "<<d_numeric<<std::endl;
//	store.Get("", d_numeric);
//	std::cout<<"'' => "<<d_numeric<<std::endl;
//	store.Get("space", a_str);
//	std::cout<<"space => '"<<a_str<<"'"<<std::endl;
//	store.Get("no value", a_str);
//	std::cout<<"no value => '"<<a_str<<"'"<<std::endl;
//	store.Get("quotes", a_str);
//	std::cout<<"quotes => '"<<a_str<<"'"<<std::endl;

	store.Get("comment", a_str);
	std::cout<<"comment => '"<<a_str<<"'"<<std::endl;
	store.Get("# -- --> */", a_str);
	std::cout<<"# -- --> */ => '"<<a_str<<"'"<<std::endl;

//	ok=store.Get(" s p a c e d ", v);
//	if(ok){
//		std::cout<<" s p a c e d  => [";
//		for(int i=0; i<v.size(); ++i){
//			a_numeric=v[i];
//			if(ok) std::cout<<a_numeric<<", ";
//		}
//		if(v.size()) std::cout<<"\b\b";
//		std::cout<<"]"<<std::endl;
//	}
//	ok=store.Get("compact", v);
//	if(ok){
//		std::cout<<"compact => [";
//		for(int i=0; i<v.size(); ++i){
//			a_numeric=v[i];
//			if(ok) std::cout<<a_numeric<<", ";
//		}
//		if(v.size()) std::cout<<"\b\b";
//		std::cout<<"]"<<std::endl;
//	}
//	ok=store.Get("array", v);
//	if(ok){
//		std::cout<<"array => [";
//		for(int i=0; i<v.size(); ++i){
//			a_numeric=v[i];
//			if(ok) std::cout<<a_numeric<<", ";
//		}
//		if(v.size()) std::cout<<"\b\b";
//		std::cout<<"]"<<std::endl;
//	}
//	ok=store.Get("\\/\\\\\\\"\\uCAFE\\uBABE\\uAB98\\uFCDE\\ubcda\\uef4A\\b\\f\\n\\r\\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?",a_str);
//	if(ok){
//		std::cout<<"\"\\/\\\\\\\"\\uCAFE\\uBABE\\uAB98\\uFCDE\\ubcda\\uef4A\\b\\f\\n\\r\\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?\" => "
//		         <<a_str<<std::endl;
//	} else {
//		std::cerr<<" couldn't find complex key "<<std::endl;
//	}
//	ok=store.Get("object",tmpstore);
//	if(ok){
//		std::cout<<"object => ";  // its empty so this prints nothing
//		tmpstore.Print();
//	}
	
	/*
	std::cout<<"store.Has(\"SlackBot\") = "<<store.Has("SlackBot")<<std::endl;
	if(store.Has("SlackBot")){
		int vmenum;
		store.Get("SlackBot",vmenum);
		std::cout<<"SlackBot is "<<vmenum<<std::endl;
	}
	*/
	
	return 0;
}
