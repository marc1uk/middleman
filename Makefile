ToolDAQPath=/home/ToolDAQApplication/ToolDAQ
#ToolDAQPath=/home/marc/LinuxSystemFiles/ToolAnalysis/ToolAnalysis/ToolDAQ

ZMQLib= -L $(ToolDAQPath)/zeromq-4.0.7/lib -lzmq
ZMQInclude= -I $(ToolDAQPath)/zeromq-4.0.7/include

BoostLib= -L $(ToolDAQPath)/boost_1_66_0/install/lib -lboost_date_time
BoostInclude= -I $(ToolDAQPath)/boost_1_66_0/install/include

CXXFLAGS= -g -std=c++11 -fdiagnostics-color=always -Wno-attributes

all: main

.phony: clean

main: main.cpp DemoClient.cpp DemoClient.h Message.cpp Message.h Store.cpp Store.h ServiceDiscovery.cpp ServiceDiscovery.h Utilities.cpp Utilities.h errnoname.c errnoname.h
	g++ $(CXXFLAGS) $^ -o $@ -I./ $(BoostInclude) $(BoostLib) $(ZMQInclude) $(ZMQLib) -lpthread

clean:
	@rm -f main
