#include "JsonParser.h"
#include <iostream>

using namespace ToolFramework; // for BStore

void JSONP::SetVerbose(bool verb){
	verbose = verb;
}

std::string JSONP::Trim(const std::string& thejson){
	// discard leading whitespace
	size_t pos=0;
	size_t len=thejson.length();
	while((pos<len) && std::isspace(thejson.at(pos))){
		++pos;
		--len;
	}
	// also discard any trailing whitespace
	while(len>0 && std::isspace(thejson.at(pos+len-1))){
		--len;
	}
	return thejson.substr(pos,len);
}

bool JSONP::iEquals(const std::string& str1, const std::string& str2){
	if(str1.length()!=str2.length()) return false;
	
	// XXX quick aside on string comparisons and length!
	// string::size()==string::length() is not necessarily the number of
	// non-null characters in the string! e.g. `str="cat"; str.resize(5);`
	// will mean `cout<<str<<endl;` will produce 'cat', but will compare
	// not equal to `str2="cat";`. This is of course true of this function,
	// but also of strings compared with ==, or string::compare!
	// strcmp(a.c_str(), b.c_str()) will return the same, though!
	// if we disabled the above check it would be less efficient,
	// but we could break on finding '\0'....
	
	for(size_t i=0; i<str1.length(); ++i){
		if(std::toupper(str1[i])!=std::toupper(str2[i])) return false;
	}
	return true;
}

bool JSONP::IsInteger(std::string& tmp){
	for(char& next : tmp) if(!std::isdigit(next) && !std::isspace(next) && next!='-') return false;
	return true;
}


bool JSONP::Parse(std::string thejson, BStore& output){

	if(verbose) std::cout<<"parsing '"<<thejson<<"'"<<std::endl;
	typechecking = output.TypeChecking();
	
	// strip leading/trailing whitespace
	thejson = Trim(thejson);
	
	// a valid json string always represents an object or an array
	// sanity check that our json is enclosed in '{ ... }' or '[ ... ]'
	if(thejson.length()<2 || not ( (thejson.front()=='{' && thejson.back()=='}') ||
	                               (thejson.front()=='[' && thejson.back()==']') ) ) {
		std::cerr<<"sanity check failed; empty json or missing outermost braces!"<<std::endl;
		return false;
	}
	
	// if object, convert to BStore
	if(thejson.front()=='{'){
		return ScanJsonObject(thejson.substr(1,thejson.length()-2), output);
	}
	
	// if array, well we're going to return it as a BStore, but a BStore needs a key
	// so just use "0" i guess....? :/
	// TODO factor this into a wrapper around ScanJsonArray so that it makes a BStore entry
	if(thejson.front()=='['){
		JsonParserResult res(typechecking);
		bool ok =  ScanJsonArray(thejson.substr(1,thejson.length()-2), res);
		if(!ok || res.type==JsonParserResultType::undefined) return false;
		switch (res.type){
			case JsonParserResultType::sints: {
				output.Set("0",res.thesints);
				break;
			}
			case JsonParserResultType::uints: {
				output.Set("0",res.theuints);
				break;
			}
			case JsonParserResultType::floats: {
				output.Set("0",res.thefloats);
				break;
			}
			case JsonParserResultType::strings: {
				output.Set("0",res.thestrings);
				break;
			}
			case JsonParserResultType::bools: {
				output.Set("0",res.thebools);
				break;
			}
			case JsonParserResultType::nulls: {
				output.Set("0",res.thenulls);
				break;
			}
			case JsonParserResultType::stores: {
				output.Set("0",res.thestore);
				break;
			}
			case JsonParserResultType::empty: {
				std::vector<std::string> emptyvec{};
				output.Set("0",emptyvec);
				break;
			}
			default:{
				std::cerr<<"unhandled case from ScanJsonArray: "<<int(res.type)<<std::endl;
				return false;
			}
		}
		return true;
	}
	
	return false;  // dummy
	
}

bool JSONP::ScanJsonArray(const std::string& thejson, JsonParserResult& result){
	if(verbose) std::cout<<"ScanJsonArray parsing '"<<thejson<<"'"<<std::endl;
	// passed a json array
	// should be sequence of comma delimited values
	
	// trivial case
	if(thejson==""){
		result.type=JsonParserResultType::empty;
		return true;
	}
	
	// empty array
	if(thejson.find_first_not_of(" ,\n")==std::string::npos){
		result.type=JsonParserResultType::empty;
		return true;
	}
	
	// json arrays are annoyingly flexible; they can be homogeneous,
	// containing ints, floats, strings, bools, nulls or objects,
	// but they can also be inhomogeneous combining elements of all these different types
	// try to encapsulate the array in the minimal datatype required
	std::vector<int64_t>& thesints = result.thesints;
	std::vector<uint64_t>& theuints = result.theuints;
	std::vector<double>& thefloats = result.thefloats;
	std::vector<std::string>& thestrings = result.thestrings;
	std::vector<int>& thebools = result.thebools;             // BStore doesn't support vector<bool>
	std::vector<std::string>& thenulls = result.thenulls;     // BStore doesn't support vector<nullptr_t>
	BStore& thestore = result.thestore;
	int theindex=0;
	
	// we'll use a set of flags while parsing to select how we try to
	// interpret the next element. As each type gets ruled out we'll
	// skip trying that interpretation for further elements
	bool all_sints=true;
	bool all_uints=true;
	bool all_floats=true;
	bool all_strings=true;
	bool all_bools=true;
	bool all_nulls=true;
	bool all_stores=true;
	
	// we can rule out some of these immediately based on characters in the json string
	// for example if it contains a quote, we can rule out all elements being numbers, bools or nulls
	if(thejson.find('"')!=std::string::npos){
		if(verbose) std::cout<<"found quote: can't be ints, floats, bools or nulls"<<std::endl;
		// not numeric, bool or null
		all_sints=false;
		all_uints=false;
		all_floats=false;
		all_bools=false;
		all_nulls=false;
		// could be strings, objects or nested arrays
	}
	// conversely if it doesn't contain a quote char, it can't be all strings
	if(thejson.find('"')==std::string::npos){
		if(verbose) std::cout<<"no quotes: can't be strings"<<std::endl;
		all_strings=false;
	}
	// we can also rule out integers, bools and nulls by the presence of a '.' character
	else if(thejson.find('.')!=std::string::npos){
		if(verbose) std::cout<<"found full stop: can't be ints, bools or nulls"<<std::endl;
		all_sints=false;
		all_uints=false;
		all_bools=false;
		all_nulls=false;
		// could be floats or strings, or arrays or objects
	}
	// rule out integers by anything other than digits and signs
	if(thejson.find_first_not_of("01234567890+-, \n")!=std::string::npos){
		if(verbose) std::cout<<"found something other than digits: can't be ints"<<std::endl;
		all_sints=false;
		all_uints=false;
	}
	// rule out doubles by anything other than numbers, signs and scientific notation characters
	if(thejson.find_first_not_of("0123456789+-.Ee^*, \n")!=std::string::npos){
		if(verbose) std::cout<<"found something other than SI characters: can't be floats"<<std::endl;
		all_floats=false;
	}
	// rule out bools and nulls by finding anything other than the corresponding characters
	if(thejson.find_first_not_of("tTrRuUeEfFaAlLsSeE, \n")!=std::string::npos){
		if(verbose) std::cout<<"found characters not in true or false; can't be bools"<<std::endl;
		all_bools=false;
	}
	if(thejson.find_first_not_of("nNuUlL, \n")!=std::string::npos){
		if(verbose) std::cout<<"found characters not in null; can't be nulls"<<std::endl;
		all_nulls=false;
	}
	// we can identify arrays and objects by enclosers, but only if we've ruled out strings
	// (otherwise these could potentially just be characters within the strings)
	if(all_strings==false && thejson.substr(1,thejson.length()-2).find("{}[]:")!=std::string::npos){
		if(verbose) std::cout<<"can't be strings and has delimiters; must be array or object"<<std::endl;
		all_sints=false;
		all_uints=false;
		all_floats=false;
		all_strings=false;
		all_bools=false;
		all_nulls=false;
	}
	
	// ok, we have our best initial determination of element types
	// the rest we'll have to figure out as we go.
	
	if(verbose) std::cout<<"scanjsonarray performing parse loop on "<<thejson<<std::endl;
	
	// scan through the array
	size_t next_start=0;
	size_t next_end=0;
	while(true){
		
		if(verbose) std::cout<<"next_start="<<next_start<<", next_end="<<next_end<<std::endl;
		if(next_end==std::string::npos || next_end==thejson.length()) break;
		if(next_end!=0) next_start=next_end+1;
		if(verbose) std::cout<<"new next_start="<<next_start<<", next_end="<<next_end<<std::endl;
		
		// find the end of the next array entry
		// note that as entry elements may be objects, nested arrays, or strings that contain commas,
		// we can't just treat it as a comma-delimited list
		bool in_string=false;
		bool escaped=false;
		std::vector<char> delimiters;
		if(verbose) std::cout<<"scanning remaining string: "
		                     <<thejson.substr(next_start,std::string::npos)<<std::endl;
		for(next_end=next_start; next_end<thejson.length(); ++next_end){
			if(verbose){
				std::cout<<"next char: "<<thejson.at(next_end)
				         <<" instring: "<<in_string<<", delimiters: ";
				for(int k=0; k<delimiters.size(); ++k){
					if(k>0) std::cout<<", ";
					std::cout<<delimiters.at(k);
				}
				std::cout<<", escaped: "<<escaped<<std::endl;
			}
			char nextchar = thejson.at(next_end);
			if(escaped){
				escaped=false;
				continue;
			}
			if(in_string && nextchar=='\\'){
				escaped=true;
				continue;
			}
			if(in_string && nextchar=='"'){
				in_string=false;
				continue;
			}
			if(!in_string && nextchar=='"'){
				in_string=true;
				continue;
			}
			if(!in_string && (nextchar=='{' || nextchar=='[')){
				delimiters.push_back(nextchar);
				continue;
			}
			if(!in_string && (nextchar=='}' || nextchar==']')){
				delimiters.pop_back();
				continue;
			}
			if(!in_string && delimiters.empty() && nextchar==','){
				break;  // end of value
			}
		}
		if(verbose) std::cout<<"broke"<<std::endl;
		// extract entry
		std::string tmp = thejson.substr(next_start, next_end-next_start);
		if(verbose) std::cout<<"next array element is "<<tmp<<std::endl;
		tmp=Trim(tmp);
		if(verbose) std::cout<<"trimmed is '"<<tmp<<"'"<<std::endl;
		// ignore empty elements?
		if(tmp.empty()) continue;
		if(tmp.front()=='{' || tmp.front()=='['){
			// found an object or array
			// since inhomogeneous element types are valid, we could have already parsed
			// several previous entries and built up e.g. a vector<int64_t>. But now,
			// since we can't append an object to that vector, we need to translate those
			// previously parsed elements into a new BStore which is sufficiently
			// generic to accommodate the new element as well
			all_sints=false;
			all_uints=false;
			all_floats=false;
			all_strings=false;
			all_bools=false;
			all_nulls=false;
			
			if(thesints.size()){
				for(int i=0; i<thesints.size(); ++i) thestore.Set(std::to_string(i), thesints.at(i));
				theindex=thesints.size();
				thesints.clear();
			}
			if(theuints.size()){
				for(int i=0; i<theuints.size(); ++i) thestore.Set(std::to_string(i), theuints.at(i));
				theindex=theuints.size();
				theuints.clear();
			}
			if(thefloats.size()){
				for(int i=0; i<thefloats.size(); ++i) thestore.Set(std::to_string(i), thefloats.at(i));
				theindex=thefloats.size();
				thefloats.clear();
			}
			if(thestrings.size()){
				for(int i=0; i<thestrings.size(); ++i) thestore.Set(std::to_string(i), thestrings.at(i));
				theindex=thestrings.size();
				thestrings.clear();
			}
			if(thebools.size()){
				for(int i=0; i<thebools.size(); ++i) thestore.Set(std::to_string(i), thebools.at(i));
				theindex=thebools.size();
				thebools.clear();
			}
			if(thenulls.size()){
				std::string emptystring="";
				for(int i=0; i<thenulls.size(); ++i) thestore.Set(std::to_string(i), emptystring);
				theindex=thenulls.size();
				thenulls.clear();
			}
		}
		if(tmp.front()=='{'){
			// add the new element
			BStore tmpstore(false,typechecking);
			bool ok =  ScanJsonObject(tmp.substr(1,tmp.length()-2), tmpstore);
			if(!ok) return false;
			thestore.Set(std::to_string(theindex),tmpstore);
			++theindex;
			continue;
		}
		if(tmp.front()=='['){
			// add the new element
			if(verbose) std::cout<<"element is array"<<std::endl;
			JsonParserResult res(typechecking);
			BStore tmpstore(false,typechecking);
			std::string tmpkey = std::to_string(theindex);
			bool ok =  ScanJsonArray(tmp.substr(1,tmp.length()-2), res);
			if(!ok || res.type==JsonParserResultType::undefined) return false;
			switch (res.type){
				case JsonParserResultType::sints: {
					thestore.Set(tmpkey,res.thesints);
					break;
				}
				case JsonParserResultType::uints: {
					thestore.Set(tmpkey,res.theuints);
					break;
				}
				case JsonParserResultType::floats: {
					thestore.Set(tmpkey,res.thefloats);
					break;
				}
				case JsonParserResultType::strings: {
					thestore.Set(tmpkey,res.thestrings);
					break;
				}
				case JsonParserResultType::bools: {
					thestore.Set(tmpkey,res.thebools);
					break;
				}
				case JsonParserResultType::nulls: {
					thestore.Set(tmpkey,res.thenulls);
					break;
				}
				case JsonParserResultType::stores: {
					thestore.Set(tmpkey,res.thestore);
					break;
				}
				case JsonParserResultType::empty: {
					std::vector<std::string> emptyvec;
					thestore.Set(tmpkey,emptyvec);
					break;
				}
				default:{
					std::cerr<<"unhandled case from ScanJsonArray: "<<int(res.type)<<std::endl;
					return false;
				}
			}
			++theindex;
			continue;
		}
		
		// ok not an object or array, try to handle it as a simpler type
		if(all_uints){
			if(verbose) std::cout<<"trying unsigned int"<<std::endl;
			// try to parse as unsigned integer, until we find something that fails
			try {
				// discard leading whitespace
				size_t startpos=0;
				size_t endpos=0;
				while(startpos<tmp.length() && std::isspace(tmp[startpos])) ++startpos;
				if(false && tmp.front()=='-'){  // always use uint64_t
					throw std::invalid_argument("not unsigned");
				} else {
					// else try to scan into uint64_t
					//uint64_t nextint = std::stoull(tmp,&endpos);
					//if(endpos!=tmp.length()) throw std::invalid_argument("extra chars");
					
					if(!IsInteger(tmp)){ throw std::invalid_argument("not integer"); }
					uint64_t nextint = strtoull(tmp.c_str(),nullptr,10); // use old version to ignore out of range errors
					if(verbose) std::cout<<"match uint"<<std::endl;
					theuints.push_back(nextint);
					continue;
				}
			}
			catch(std::invalid_argument& e){
				// swap any already parsed integers to the signed integer array
				if(verbose){ std::cout<<"shifting unsigned ints to signed ints"<<std::endl; }
				if(theuints.size() && thesints.size()){
					// sanity check: shouldn't ever happen
					std::cerr<<"parsing error; transferring unsigned ints into non-empty ints!"<<std::endl;
					return false;
				}
				for(uint64_t& anint : theuints) thesints.push_back(anint);
				theuints.clear();
				all_uints = false;
				all_sints = true;
			}
		}
		if(all_sints){
			if(verbose) std::cout<<"trying signed int"<<std::endl;
			// try to parse as signed integer, until we find something that fails
			try {
				size_t endpos=0;
				int64_t nextint = std::stoll(tmp,&endpos);
				if(endpos!=tmp.length()) throw std::invalid_argument("extra chars");
				if(verbose) std::cout<<"match sint"<<std::endl;
				thesints.push_back(nextint);
				continue;
			}
			catch(std::invalid_argument& e){
				// swap any already parsed integers to the signed integer array
				if(verbose){ std::cout<<"shifting signed ints to floats"<<std::endl; }
				if(theuints.size() && thesints.size()){
					// sanity check: shouldn't ever happen
					std::cerr<<"parsing error; transferring signed ints into non-empty floats!"<<std::endl;
					return false;
				}
				for(int64_t& anint : thesints) thefloats.push_back(anint);
				thesints.clear();
				all_sints = false;
				all_floats = true;
			}
		}
		if(all_floats){
			if(verbose) std::cout<<"trying float"<<std::endl;
			// try to parse as a double until we find something that fails
			try {
				size_t endpos=0;
				double nextfloat = std::stod(tmp,&endpos);
				if(endpos!=tmp.length()) throw std::invalid_argument("extra chars");
				if(verbose) std::cout<<"match float"<<std::endl;
				thefloats.push_back(nextfloat);
				continue;
			}
			catch(std::invalid_argument& e){
				// must be inhomogeneous types. transfer to store
				if(verbose){ std::cout<<"shifting floats to store"<<std::endl; }
				if(thefloats.size() && theindex>0){
					// sanity check: shouldn't ever happen
					std::cerr<<"parsing error; transferring floats into non-empty store!"<<std::endl;
					return false;
				}
				for(int i=0; i<thefloats.size(); ++i){
					thestore.Set(std::to_string(i), thefloats.at(i));
				}
				theindex=thefloats.size();
				thefloats.clear();
				all_floats = false;
				all_stores = true;
			}
		}
		if(all_bools){
			if(verbose) std::cout<<"trying bool"<<std::endl;
			if(iEquals(tmp,"TRUE")){
				thebools.push_back(1);
				if(verbose) std::cout<<"match bool"<<std::endl;
				continue;
			} else if(iEquals(tmp,"FALSE")){
				thebools.push_back(0);
				if(verbose) std::cout<<"match bool"<<std::endl;
				continue;
			} else {
				// not all bools
				if(verbose){ std::cout<<"shifting bools to stores"<<std::endl; }
				// transfer current contents to Store
				if(thebools.size() && theindex>0){
					// sanity check: shouldn't ever happen
					std::cerr<<"parsing error; transferring bools into non-empty store!"<<std::endl;
					return false;
				}
				for(int i=0; i<thebools.size(); ++i){
					thestore.Set(std::to_string(i), thebools.at(i));
				}
				theindex = thebools.size();
				thebools.clear();
				all_bools = false;
				all_stores = true;
			}
		}
		if(all_nulls){
			if(verbose) std::cout<<"trying null"<<std::endl;
			if(iEquals(tmp,"null")){
				thenulls.resize(thenulls.size()+1);
				if(verbose) std::cout<<"match null"<<std::endl;
				continue;
			} else {
				// not all nulls
				if(verbose){ std::cout<<"shifting nulls to stores"<<std::endl; }
				// transfer current contents to Store
				if(thenulls.size() && theindex>0){
					// sanity check: shouldn't ever happen
					std::cerr<<"parsing error; transferring nulls into non-empty store!"<<std::endl;
					return false;
				}
				for(int i=0; i<thenulls.size(); ++i){
					std::string emptystring="";
					thestore.Set(std::to_string(i), emptystring);
				}
				theindex=thenulls.size();
				thenulls.clear();
				all_nulls = false;
				all_stores = true;
			}
		}
		if(all_strings){
			if(verbose) std::cout<<"trying string"<<std::endl;
			// check it looks like a string
			if(tmp.length()>1 && tmp.front()=='"' && tmp.back()=='"'){
				// remove the enclosing quotes
				thestrings.push_back(tmp.substr(1,tmp.length()-2));
				if(verbose) std::cout<<"match string"<<std::endl;
				continue;
			} else {
				// doesn't look like a json string. need to use stores
				if(verbose){ std::cout<<"shifting strings to stores"<<std::endl; }
				// transfer current contents to Stores
				if(thestrings.size() && theindex>0){
					// sanity check: shouldn't ever happen
					std::cerr<<"parsing error; transferring nulls into non-empty store!"<<std::endl;
					return false;
				}
				for(int i=0; i<thestrings.size(); ++i){
					thestore.Set(std::to_string(i),thestrings.at(i));
				}
				theindex=thestrings.size();
				thestrings.clear();
				all_strings = false;
				all_stores = true;
			}
		}
		if(all_stores){
			if(verbose) std::cout<<"falling back to store"<<std::endl;
			// we already checked that this key is not an object or array
			// before moving on to all_uints, all_sints, etc etc
			// so we just need to put this primitive into a BStore entry:
			bool ok = ScanJsonPrimitive(tmp, std::to_string(theindex), thestore);
			if(verbose) std::cout<<"returned "<<ok<<std::endl;
			if(!ok) return false;
			++theindex;
			continue;
		}
		// shouldn't get here
		std::cerr<<"Nothing to parse element "<<tmp<<std::endl;
		return false;
		
	}
	
	// determine the type of our return
	int typesset=0;
	if(thesints.size())  { result.type = JsonParserResultType::sints;   ++typesset; }
	if(theuints.size())  { result.type = JsonParserResultType::uints;   ++typesset; }
	if(thefloats.size()) { result.type = JsonParserResultType::floats;  ++typesset; }
	if(thestrings.size()){ result.type = JsonParserResultType::strings; ++typesset; }
	if(thebools.size())  { result.type = JsonParserResultType::bools;   ++typesset; }
	if(thenulls.size())  { result.type = JsonParserResultType::nulls;   ++typesset; }
	if(theindex>0)       { result.type = JsonParserResultType::stores;  ++typesset; }
	if(typesset!=1){
		std::cerr<<"multiple ("<<typesset<<") types in return from ScanJsonArray!"<<std::endl;
		std::cerr<<"for json '"<<thejson<<"', types:"
			 <<"uints:"<<theuints.size()
			 <<"sints:"<<thesints.size()
			 <<"floats:"<<thefloats.size()
			 <<"strings:"<<thestrings.size()
			 <<"bools:"<<thebools.size()
			 <<"nulls:"<<thenulls.size()
			 <<"store:"<<theindex
			 <<std::endl;
		return false;
	}
	// one last thing; if we used a store, put the number of elements in it.
	if(theindex) thestore.Set("#",theindex);
	// TODO make PR to add a 'Count' method to BStore
	return true;
	
}

bool JSONP::ScanJsonPrimitive(std::string thejson, std::string thekey, BStore& outstore){
	if(verbose) std::cout<<"ScanJsonPrimitive scanning '"<<thejson<<"'"<<std::endl;
	thejson=Trim(thejson);
	
	if(thejson.front()=='{' || thejson.front()=='['){
		std::cerr<<"Warning! ScanJsonPrimitive called with object or array!"<<std::endl;
		return false;
	}
	
	try {
		if(verbose) std::cout<<"try int"<<std::endl;
		
		// discard leading whitespace
		size_t startpos=0;
		size_t endpos=0;
		while(startpos<thejson.length() && std::isspace(thejson[startpos])) ++startpos;
		if(false && thejson.front()=='-'){ // always use uint64_t
			// if negative use temporary int64_t
			int64_t nextint = std::stoll(thejson,&endpos);
			if(endpos!=thejson.length()) throw std::invalid_argument("extra chars");
			outstore.Set(thekey,nextint);
			return true;
		} else {
			// else use temporary uint64_t
			//uint64_t nextint = std::stoull(thejson,&endpos);
			//if(endpos!=thejson.length()) throw std::invalid_argument("extra chars");
			
			if(!IsInteger(thejson)){ throw std::invalid_argument("not integer"); }
			uint64_t nextint = strtoull(thejson.c_str(),nullptr,10); // use old version to ignore out of range errors
			outstore.Set(thekey,nextint);
			return true;
		}
	}
	catch(std::invalid_argument& e){
		// not an int
		if(verbose) std::cout<<"not int"<<std::endl;
	}
	try {
		if(verbose) std::cout<<"try float"<<std::endl;
		size_t endpos=0;
		double nextfloat = std::stod(thejson,&endpos);
		if(endpos!=thejson.length()) throw std::invalid_argument("extra chars");
		outstore.Set(thekey,nextfloat);
		return true;
	}
	catch(std::invalid_argument& e){
		// not a float
		if(verbose) std::cout<<"not float"<<std::endl;
	}
	if(verbose) std::cout<<"try bool"<<std::endl;
	if(iEquals(thejson,"TRUE")){
		bool val=true;
		if(verbose) std::cout<<"match bool"<<std::endl;
		outstore.Set(thekey,val);
		return true;
	}
	if(iEquals(thejson,"FALSE")){
		if(verbose) std::cout<<"match bool"<<std::endl;
		bool val=false;
		outstore.Set(thekey,val);
		return true;
	}
	if(verbose) std::cout<<"try null"<<std::endl;
	if(iEquals(thejson,"null")){
		if(verbose) std::cout<<"match null"<<std::endl;
		std::string val="";
		outstore.Set(thekey,val);
		return true;
	}
	if(verbose) std::cout<<"try string"<<std::endl;
	if(thejson.length()>1 && thejson.front()=='"' && thejson.back()=='"'){
		if(verbose) std::cout<<"match string"<<std::endl;
		std::string substr = thejson.substr(1,thejson.length()-2);
		outstore.Set(thekey,substr);
		return true;
	}
	std::cerr<<"No handler for string "<<thejson<<" in ScanJsonPrimitive!"<<std::endl;
	
	return false;
	
}

bool JSONP::ScanJsonObject(std::string thejson, BStore& outstore){
	if(verbose) std::cout<<"ScanJsonObject scanning '"<<thejson<<"'"<<std::endl;
	// passed a json object
	// should be sequence of comma delimited key-value pairs,
	// with string keys separated from values by colons
	
	// technically JSON objects and arrays may have a trailing comma
	thejson=Trim(thejson);
	if(thejson.back()==',') thejson.pop_back();
	thejson=Trim(thejson);
	
	// trivial case
	if(thejson=="") return true;
	
	// scan through the array of key-value pairs
	size_t next_start=0;
	size_t next_end=std::string::npos;
	bool key=true; // alternate key and value
	std::string next_key;
	bool escaped=false;
	while(true){
		// find the end of the next array entry
		// note that as entry elements may be objects, nested arrays, or strings that contain commas,
		// we can't just treat it as a comma-delimited list
		bool in_string=false;
		std::vector<char> delimiters;
		if(verbose) std::cout<<"scanning remaining string: "
		                     <<thejson.substr(next_start,std::string::npos)<<std::endl;
		for(next_end=next_start; next_end<thejson.length(); ++next_end){
			if(verbose){
				std::cout<<"key: "<<key<<", next char: "<<thejson.at(next_end)
				         <<" instring: "<<in_string<<", delimiters: ";
				for(int k=0; k<delimiters.size(); ++k){
					if(k>0) std::cout<<", ";
					std::cout<<delimiters.at(k);
				}
				std::cout<<", escaped: "<<escaped<<std::endl;
			}
			char& nextchar = thejson.at(next_end);
			if(escaped){
				escaped=false;
				continue;
			}
			if(in_string && nextchar=='\\'){
				escaped=true;
				continue;
			}
			if(in_string && nextchar=='"'){
				in_string=false;
				if(key && delimiters.empty()){
					++next_end; // include the closing quote
					if(verbose) std::cout<<"end of key at next_end="<<next_end<<std::endl;
					break;   // end of key
				}
				continue;
			}
			if(!in_string && nextchar=='"'){
				in_string=true;
				continue;
			}
			if(!in_string && (nextchar=='{' || nextchar=='[')){
				delimiters.push_back(nextchar);
				continue;
			}
			if(!in_string && (nextchar=='}' || nextchar==']')){
				delimiters.pop_back();
				continue;
			}
			if(!in_string && delimiters.empty() && nextchar==','){
				if(key){
					std::cerr<<"found comma without property!"<<std::endl;
					return false;
				}
				break;  // end of value
			}
			if(!in_string && delimiters.empty() && nextchar==':'){
				if(key){
					std::cerr<<"found : without key!"<<std::endl;
					return false;
				}
			}
		}
		if(verbose) std::cout<<"next element from "<<next_start<<" to "<<next_end<<std::endl;
		// extract entry
		std::string tmp = thejson.substr(next_start, next_end-next_start);
		if(verbose) std::cout<<"broke loop: '"<<tmp<<"'"<<std::endl;
		tmp=Trim(tmp);
		
		// if processing a key record it and continue to next loop
		if(key){
			// ignore duplicated delimiters
			if(tmp.empty()) continue;
			next_key=tmp;
			if(verbose) std::cout<<"it key"<<std::endl;
			// sanity checks, key should be a string
			if(next_key.front()!='"' || next_key.back()!='"'){
				std::cerr<<"next key '"<<next_key<<"' is not a string?"<<std::endl;
				return false;
			}
			// strip them off for use as key in output BStore
			next_key = next_key.substr(1,next_key.length()-2);
			// swallow any whitespace and ':' separating key from value
			int cc=0;
			while(next_start!=thejson.length()){
				if(std::isspace(thejson.at(next_end))){ ++next_end; }
				else if(thejson.at(next_end)==':'){ ++next_end; ++cc; }
				else break;
			}
			if(next_start==thejson.length()){
				std::cerr<<"encountered end of json with no value for key "<<next_key<<std::endl;
				return false;
			} else if(cc!=1){
				std::cerr<<"didn't find key-value separator in json object?"<<std::endl;
				return false;
			}
			--next_end; // backtrack one
			if(verbose) std::cout<<"sanity checks passed"<<std::endl;
		} else {
			
			if(verbose) std::cout<<"it value"<<std::endl;
			// if not processing a key, parse the value
			bool trytoparse=true;
			if(trytoparse && tmp.front()=='{'){
				// it's an object, recursively parse it
				BStore res(false,typechecking);
				bool ok =  ScanJsonObject(tmp.substr(1,tmp.length()-2), res);
				if(!ok) return false;
				outstore.Set(next_key,res);
				trytoparse=false;
			}
			if(verbose && trytoparse) std::cout<<"not object"<<std::endl;
			if(trytoparse && tmp.front()=='['){
				if(verbose) std::cout<<"it array"<<std::endl;
				// it's an array, call ScanJsonArray to parse it to something suitable
				JsonParserResult res(typechecking);
				bool ok =  ScanJsonArray(tmp.substr(1,tmp.length()-2), res);
				if(verbose) std::cout<<"parse array ret:"<<ok<<std::endl;
				if(!ok || res.type==JsonParserResultType::undefined) return false;
				// add to the store depending on the type ScanJsonArray found it to be
				switch (res.type){
					case JsonParserResultType::sints: {
						if(verbose) std::cout<<"array was of signed ints"<<std::endl;
						outstore.Set(next_key,res.thesints);
						break;
					}
					case JsonParserResultType::uints: {
						if(verbose) std::cout<<"array was of unsigned ints"<<std::endl;
						outstore.Set(next_key,res.theuints);
						break;
					}
					case JsonParserResultType::floats: {
						if(verbose) std::cout<<"array was of floats"<<std::endl;
						outstore.Set(next_key,res.thefloats);
						break;
					}
					case JsonParserResultType::strings: {
						if(verbose) std::cout<<"array was of strings"<<std::endl;
						outstore.Set(next_key,res.thestrings);
						break;
					}
					case JsonParserResultType::bools: {
						if(verbose) std::cout<<"array was of bools"<<std::endl;
						outstore.Set(next_key,res.thebools);
						break;
					}
					case JsonParserResultType::nulls: {
						if(verbose) std::cout<<"array was of nulls"<<std::endl;
						outstore.Set(next_key,res.thenulls);
						break;
					}
					case JsonParserResultType::stores: {
						if(verbose) std::cout<<"array was of inhomogeneous type (convered to BStore)"<<std::endl;
						outstore.Set(next_key,res.thestore);
						break;
					}
					case JsonParserResultType::empty: {
						if(verbose) std::cout<<"array was empty"<<std::endl;
						std::vector<std::string> emptyvec{};
						outstore.Set(next_key,emptyvec);
						break;
					}
					default:{
						std::cerr<<"unhandled case from ScanJsonArray: "<<int(res.type)<<std::endl;
						return false;
					}
				}
				trytoparse=false;
			}
			if(verbose && trytoparse) std::cout<<"not array"<<std::endl;
			
			if(trytoparse){
				// try int
				// first discard leading whitespace
				size_t startpos=0;
				size_t endpos=0;
				while(startpos<tmp.length() && std::isspace(tmp[startpos])) ++startpos;
				try {
					if(false && tmp.front()=='-'){ // always use uint64_t
						// if negative try temporary int64_t
						int64_t nextint = std::stoll(tmp,&endpos);
						if(endpos!=tmp.length()) throw std::invalid_argument("extra chars");
						outstore.Set(next_key,nextint);
					} else {
						// else try temporary uint64_t
						//uint64_t nextint = std::stoull(tmp,&endpos);
						//if(endpos!=tmp.length()) throw std::invalid_argument("extra chars");
						
						if(!IsInteger(tmp)){ throw std::invalid_argument("not integer"); }
						uint64_t nextint = strtoull(tmp.c_str(),nullptr,10); // use old version to ignore out of range errors
						outstore.Set(next_key,nextint);
					}
					trytoparse=false;
				}
				catch(std::invalid_argument& e){
					// not an int
				}
			}
			if(verbose && trytoparse) std::cout<<"not int"<<std::endl;
			if(trytoparse){
				// try double
				try {
					size_t endpos=0;
					double nextfloat = std::stod(tmp,&endpos);
					if(endpos!=tmp.length()) throw std::invalid_argument("extra chars");
					outstore.Set(next_key,nextfloat);
					trytoparse=false;
				}
				catch(std::invalid_argument& e){
					// not a double
				}
			}
			if(verbose && trytoparse) std::cout<<"not float"<<std::endl;
			if(trytoparse){
				// try bool
				if(iEquals(tmp,"TRUE")){
					bool val=true;
					outstore.Set(next_key,val);
					trytoparse=false;
				} else if(iEquals(tmp,"FALSE")){
					bool val=false;
					outstore.Set(next_key,val);
					trytoparse=false;
				}
			}
			if(verbose && trytoparse) std::cout<<"not bool"<<std::endl;
			if(trytoparse){
				// try null
				if(iEquals(tmp,"null")){
					std::string nullstring;
					outstore.Set(next_key,nullstring);
					trytoparse=false;
				}
			}
			if(verbose && trytoparse) std::cout<<"not null"<<std::endl;
			if(trytoparse){
				// try string
				if(tmp.length()>1 && tmp.front()=='"' && tmp.back()=='"'){
					std::string substr = tmp.substr(1,tmp.length()-2);
					outstore.Set(next_key,substr);
					trytoparse=false;
				}
			}
			if(verbose && trytoparse) std::cout<<"not string"<<std::endl;
			if(trytoparse){
				// try... primitive?
				// is this just a factored-out duplicate of all the above lines?
				BStore astore(false,typechecking);
				bool ok = ScanJsonPrimitive(tmp, next_key, astore);
				if(!ok) return false;
				trytoparse=false;
			}
			if(trytoparse){
				// shouldn't get here
				std::cerr<<"Nothing to parse element "<<tmp<<std::endl;
				return false;
			}
		}
		
		key = !key;
		if(verbose) std::cout<<"updating iterators"<<std::endl;
		if(next_end==thejson.length()) break;
		next_start=next_end+1;
		while(next_start<thejson.length() && std::isspace(thejson[next_start])) ++next_start;
	}
	if(verbose) std::cout<<"parsing object done"<<std::endl;
	
	return true;
	
}

