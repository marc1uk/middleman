#ifndef JSONP_H
#define JSONP_H
#include <locale>  // std::isspace
#include "BStore.h"
#include <string>
#include <vector>
// https://stackoverflow.com/a/27425792/3544936

using namespace ToolFramework; // for BStore

enum class JsonParserResultType { ints, floats, strings, bools, nulls, stores, empty, undefined };
struct JsonParserResult {
	std::vector<int> theints{};
	std::vector<double> thefloats{};
	std::vector<std::string> thestrings{};
	std::vector<int> thebools{};
	std::vector<std::string> thenulls{};
	std::vector<BStore> thestores{};
	JsonParserResultType type=JsonParserResultType::undefined;
};

class JSONP {
	public:
	JSONP(){};
	~JSONP(){};
	
	bool Parse(std::string thejson, BStore& output);
	std::string Trim(const std::string& thejson);
	bool iEquals(const std::string& str1, const std::string& str2);
	void SetVerbose(bool);
	
	private:
	bool ScanJsonArray(const std::string& thejson, JsonParserResult& result);
	bool ScanJsonObjectPrimitive(std::string thejson, BStore& outstore);
	bool ScanJsonObject(std::string thejson, BStore& outstore);
	int verbose=0;
	bool typechecking=false;
	
};
#endif
