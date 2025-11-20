#include <iostream>
#include <fstream>
#include <signal.h>
#include <DAQInterface.h>
#include <chrono>

using namespace ToolFramework;

bool running = false;
void stopSignalHandler(int _ignored){
  running = false;
}

std::ofstream outfile;

// class for automating functions from slowcontrol
class AutomatedFunctions {
  
  public:
  AutomatedFunctions(DAQInterface* in_DAQ_inter){
    DAQ_inter=in_DAQ_inter;
  };
  
  DAQInterface* DAQ_inter;
  void TestAlert_func(const char* alert, const char* payload){
    time_t now = std::chrono::high_resolution_clock::to_time_t(std::chrono::high_resolution_clock::now());
    std::string payloadstr = "";
    if(payload!=nullptr) payloadstr = payload;
    std::cout<<"recevied a '"<<alert<<"' alert with payload '"<<payloadstr<<"' at "<<ctime(&now)<<std::endl;
    outfile<<"recevied a '"<<alert<<"' alert with payload '"<<payloadstr<<"' at "<<ctime(&now)<<std::endl;
  }
  
};

int main(){

  // listen for SIGINT
  signal((int) SIGINT, stopSignalHandler);
  
  outfile.open("alertlogs.txt");
  
  int verbose=1;
  
  ////////////////////////////// setup /////////////////////////////////
  
  std::string Interface_configfile = "./InterfaceConfig";
  
  std::cout<<"Constructing DAQInterface"<<std::endl;
  DAQInterface DAQ_inter(Interface_configfile);
  std::string device_name = DAQ_inter.GetDeviceName(); //name of my device
  
  std::cout<<"Constructing an AutomatedFunctions helper class to encapsulate callback functions"<<std::endl;
  AutomatedFunctions automated_functions(&DAQ_inter);
  // N.B. callback functions can simply be ordinary free functions, or they may be member functions
  // as in this case, where we define an 'AutomatedFunctions' helper class to hold them.
  // The advantage of using a helper class such as this is that the helper class can keep a handle
  // to the DAQInterface instance, which callback functions may then use to send logs, alarms,
  // update the service status, and so on.
  
  //////////////////////////////////////////////////////////////////////
  
  std::cout<<"Updating service status to 'Initialising'"<<std::endl;
  DAQ_inter.sc_vars["Status"]->SetValue("Initialising"); //setting status message
  
  std::cout<<"Registering callback function 'AutomatedFunctions::TestAlert_func' to be invoked on alert 'TestAlert'..."<<std::flush;
  // if the DAQ sends out a global "TestAlert" alert, the registered callback function will be automatically invoked
  std::vector<std::string> alertnames{"TestAlert","RunStart","ChangeConfig","RunStop","LEDTrigger","SoftTrigger"};
  for(std::string& alert_name : alertnames){
    DAQ_inter.AlertSubscribe(alert_name,  std::bind(&AutomatedFunctions::TestAlert_func, automated_functions,  std::placeholders::_1, std::placeholders::_2));
  }
  std::cout<<"Done"<<std::endl;
  
  ///////////////////////////////////////////////////////////////////////////
  
  std::cout<<"Updating service status to 'Ready'"<<std::endl;
  DAQ_inter.sc_vars["Status"]->SetValue("Ready");
   
  if(verbose) std::cout<<"Registering 'Quit' button..."<<std::flush;
  DAQ_inter.sc_vars.Add("Quit",BUTTON);
  DAQ_inter.sc_vars["Quit"]->SetValue(false);
  if(verbose) std::cout<<"Done"<<std::endl;
  
  running=true;
  
  while(running){ // run until user clicks 'Quit' slow control
       
    // check if the quit button has been pressed
    running=(!DAQ_inter.sc_vars["Quit"]->GetValue<bool>()); 
    
    usleep(1000);
    
  } // end of program loop
  
  
  std::cout<<"Application terminated"<<std::endl;
  DAQ_inter.sc_vars["Status"]->SetValue("Terminated");
  
  outfile.close();
  
  return 0;
  
}
