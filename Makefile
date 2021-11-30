ToolDAQPath=/home/ToolDAQApplication/ToolDAQ

PostgresLib= -L $(ToolDAQPath)/libpqxx-6.4.5/install/lib -lpqxx -L /usr/pgsql-12/lib -lpq
PostgresInclude= -I $(ToolDAQPath)/libpqxx-6.4.5/install/include -I /usr/pgsql-12/include

ZMQLib= -L $(ToolDAQPath)/zeromq-4.0.7/lib -lzmq
ZMQInclude= -I $(ToolDAQPath)/zeromq-4.0.7/include

#ToolDAQPath=/home/marc/LinuxSystemFiles/ToolAnalysis/ToolAnalysis/ToolDAQ

#PostgresLib= -L $(ToolDAQPath)/pqxx/install/lib -lpqxx -L /usr/pgsql-12/lib -lpq
#PostgresInclude= -I $(ToolDAQPath)/pqxx/install/include -I /opt/rh/rh-postgresql12/root/use/include

BoostLib= -L $(ToolDAQPath)/boost_1_66_0/install/lib -lboost_date_time
BoostInclude= -I $(ToolDAQPath)/boost_1_66_0/install/include

CXXFLAGS= -g -std=c++11 -fdiagnostics-color=always -Wno-attributes -O3

all: main

.phony: clean

main: main.cpp $(filter-out main.o, $(patsubst %.cpp, %.o, $(wildcard *.cpp)))
	g++ $(CXXFLAGS) $^ -o $@ -I./ $(PostgresInclude) $(BoostInclude) $(PostgresLib) $(ZMQInclude) $(BoostLib) $(ZMQLib) -lpthread

%.o: %.cpp %.h
	g++ $(CXXFLAGS) -c -fPIC $< -o $@ -I./ $(PostgresInclude) $(BoostInclude) $(PostgresLib) $(ZMQInclude) $(BoostLib) $(ZMQLib) -lpthread

clean:
	@rm -f main *.o
