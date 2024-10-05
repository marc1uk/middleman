#!/bin/bash
Dependencies=/opt

# add Dependencies from ToolFramework
export LD_LIBRARY_PATH=${Dependencies}/zeromq-4.0.7/lib:${Dependencies}/boost_1_66_0/install/lib:${Dependencies}/libpqxx-6.4.5/install/lib:${Dependencies}/ToolFrameworkCore/lib:${Dependencies}/ToolDAQFramework/lib:$LD_LIBRARY_PATH

