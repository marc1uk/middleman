#ifndef JSONP_H
#define JSONP_H
#include <locale>  // std::isspace
#include "BStore.h"
#include <string>
#include <vector>
// https://stackoverflow.com/a/27425792/3544936

using namespace ToolFramework; // for BStore

enum class JsonParserResultType { sints, uints, floats, strings, bools, nulls, stores, empty, undefined };
struct JsonParserResult {
	std::vector<int64_t> thesints{};
	std::vector<uint64_t> theuints{};
	std::vector<double> thefloats{};
	std::vector<std::string> thestrings{};
	std::vector<int> thebools{};
	std::vector<std::string> thenulls{};
	std::vector<BStore> thestores{};
	JsonParserResultType type=JsonParserResultType::undefined;
	
	JsonParserResult() {};
};

class JSONP {
	public:
	JSONP(){};
	~JSONP(){};
	
	// NOTE: output BStore should have typechecking DISABLED as the specific types used
	// may not be guaranteed! (i.e. what specific type of integer is {"anum":5}? uint32_t? int64_t?)
	// specifically all integers will be uint64_t unless negative, then int64_t.
	// all non-integral numbers will be double.)
	// however the typechecking propagation previously was not complete and would change during parsing...
	bool Parse(std::string thejson, BStore& output);
	std::string Trim(const std::string& thejson);
	bool iEquals(const std::string& str1, const std::string& str2);
	void SetVerbose(bool);
	
	private:
	bool ScanJsonArray(const std::string& thejson, JsonParserResult& result);
	bool ScanJsonObjectPrimitive(std::string thejson, BStore& outstore);
	bool ScanJsonObject(std::string thejson, BStore& outstore);
	int verbose=0;
	
};
#endif
