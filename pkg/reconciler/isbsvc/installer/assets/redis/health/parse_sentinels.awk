/ip/ {FOUND_IP=1}
/port/ {FOUND_PORT=1}
/runid/ {FOUND_RUNID=1}
!/ip|port|runid/ {
  if (FOUND_IP==1) {
    IP=$1; FOUND_IP=0;
  }
  else if (FOUND_PORT==1) {
    PORT=$1;
    FOUND_PORT=0;
  } else if (FOUND_RUNID==1) {
    printf "\nsentinel known-sentinel mymaster %s %s %s", IP, PORT, $0; FOUND_RUNID=0;
  }
}