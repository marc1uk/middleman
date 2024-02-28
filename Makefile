Dependencies=/opt

PostgresLib= -L $(Dependencies)/libpqxx-6.4.5/install/lib -lpqxx -L `pg_config --libdir` -lpq
PostgresInclude= -I $(Dependencies)/libpqxx-6.4.5/install/include -I `pg_config --includedir`
#PostgresLib= -L $(Dependencies)/libpqxx-6.4.5/install/lib -lpqxx `pg_config --ldflags` `pg_config --libs`

ZMQLib= -L $(Dependencies)/zeromq-4.0.7/lib -lzmq
ZMQInclude= -I $(Dependencies)/zeromq-4.0.7/include

BoostLib= -L $(Dependencies)/boost_1_66_0/install/lib -lboost_date_time
BoostInclude= -I $(Dependencies)/boost_1_66_0/install/include

CXXFLAGS= -g -fdiagnostics-color=always -Wno-attributes -O3

all: middleman

.phony: clean

middleman: main.cpp $(filter-out main.o, $(patsubst %.cpp, %.o, $(wildcard *.cpp)))
	g++ $(CXXFLAGS) $^ -o $@ -I./ $(PostgresInclude) $(BoostInclude) $(PostgresLib) $(ZMQInclude) $(BoostLib) $(ZMQLib) -lpthread

%.o: %.cpp %.h
	g++ $(CXXFLAGS) -c -fPIC $< -o $@ -I./ $(PostgresInclude) $(BoostInclude) $(PostgresLib) $(ZMQInclude) $(BoostLib) $(ZMQLib) -lpthread

clean:
	@rm -f main *.o
