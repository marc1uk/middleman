# minimal middleman tester
Dependencies=/opt

ZMQLib= -L $(Dependencies)/zeromq-4.0.7/lib -lzmq
ZMQInclude= -I $(Dependencies)/zeromq-4.0.7/include/

BoostLib= -L $(Dependencies)/boost_1_66_0/install/lib -lboost_date_time -lboost_serialization -lboost_iostreams
BoostInclude= -I $(Dependencies)/boost_1_66_0/install/include

ToolDAQLib= -L $(Dependencies)/ToolDAQFramework/lib   -lDAQDataModelBase -lServiceDiscovery -lDAQStore
ToolDAQInclude= -I $(Dependencies)/ToolDAQFramework/include

ToolFrameworkLib= -L $(Dependencies)/ToolFrameworkCore/lib -lStore -lDataModelBase
ToolFrameworkInclude= -I $(Dependencies)/ToolFrameworkCore/include

all: MessageMiddleman SendAlert ReceiveAlert

MessageMiddleman: MessageMiddleman.cpp lib/libDAQInterface.so
        g++ -O3  -Wpedantic -std=c++11 $< -o $@ -I ./include/ -L lib/ -lDAQInterface -lpthread $(ToolDAQInclude) $(ToolDAQLib) $(ToolFrameworkInclude) $(ToolFrameworkLib) $(BoostInclude) $(ZMQInclude) $(ZMQLib) $(ToolDAQLib) $(BoostLib) $(ToolDAQLib)

SendAlert: SendAlert.cpp lib/libDAQInterface.so
        g++ -O3  -Wpedantic -std=c++11 $< -o $@ -I ./include/ -L lib/ -lDAQInterface -lpthread $(ToolDAQInclude) $(ToolDAQLib) $(ToolFrameworkInclude) $(ToolFrameworkLib) $(BoostInclude) $(ZMQInclude) $(ZMQLib) $(ToolDAQLib) $(BoostLib) $(ToolDAQLib)
        
ReceiveAlert: ReceiveAlert.cpp lib/libDAQInterface.so
        g++ -O3  -Wpedantic -std=c++11 $< -o $@ -I ./include/ -L lib/ -lDAQInterface -lpthread $(ToolDAQInclude) $(ToolDAQLib) $(ToolFrameworkInclude) $(ToolFrameworkLib) $(BoostInclude) $(ZMQInclude) $(ZMQLib) $(ToolDAQLib) $(BoostLib) $(ToolDAQLib)
