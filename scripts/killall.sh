
ps -aux | grep runcl | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null
ps -aux | grep rundb | awk '{print $2}' | xargs kill -9 2>/dev/null 1>/dev/null
    ssh 9.39.242.133 "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 9.39.242.133 "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 9.39.242.175 "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 9.39.242.175 "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 9.39.242.189 "ps -aux | grep rundb | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
    ssh 9.39.242.189 "ps -aux | grep runcl | awk '{print \$2}' | xargs kill -9" 2>/dev/null 1>/dev/null
