#include <iostream>
#include <DAQInterface.h>

using namespace ToolFramework;

int main(){

  int verbose=1;
  
  ////////////////////////////// setup /////////////////////////////////
  
  std::string Interface_configfile = "./InterfaceConfig";
  //std::string database_name = "daq";
  
  std::cout<<"Constructing DAQInterface"<<std::endl;
  DAQInterface DAQ_inter(Interface_configfile);
  std::string device_name = DAQ_inter.GetDeviceName(); //name of my device
  
  std::cout<<"sending test 'TestAlert' alert"<<std::endl;
  bool ok = DAQ_inter.AlertSend("TestAlert","TEST!");
  std::cout<<"send ok: "<<ok<<std::endl;

  return 0;
  
}
