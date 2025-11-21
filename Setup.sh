#!/bin/bash
Dependencies=/opt
#export PS1='${debian_chroot:+($debian_chroot)}\[\033[35;2m\]\u@\h\[\033[02;31;22m\][\t]\[\033[00m\]:\[\033[00;36m\]\w\[\033[00m\]\$ '
export PS1='${debian_chroot:+($debian_chroot)}\[\033[35;2;1m\]\u@\h\[\033[00m\]:\[\033[00;36m\]\w\[\033[00m\]\$ '

# add Dependencies from ToolFramework
export LD_LIBRARY_PATH=${Dependencies}/zeromq-4.0.7/lib:${Dependencies}/boost_1_66_0/install/lib:${Dependencies}/libpqxx-6.4.5/install/lib:${Dependencies}/ToolFrameworkCore/lib:${Dependencies}/ToolDAQFramework/lib:$LD_LIBRARY_PATH

