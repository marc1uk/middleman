#!/bin/bash
ToolDAQapp=/home/ToolDAQApplication

#Application path location of applicaiton
export LD_LIBRARY_PATH=${ToolDAQapp}/ToolDAQ/zeromq-4.0.7/lib:${ToolDAQapp}/ToolDAQ/boost_1_66_0/install/lib:${ToolDAQapp}/ToolDAQ/libpqxx-6.4.5/install/lib:/usr/pgsql-12/lib:$LD_LIBRARY_PATH

export PKG_CONFIG_PATH=/usr/pgsql-12/lib/pkgconfig:$PKG_CONFIG_PATH
export PATH=/usr/pgsql-12/bin:$PATH
