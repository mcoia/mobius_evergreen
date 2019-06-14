#/bin/bash
numweb=$(pgrep -fc apache2)
numnginx=$(pgrep -fc nginx)
today=$(date '+%Y-%m-%d')
numdrones=$(egrep $today /openils/var/log/osrfsys.log | egrep -i "no children" | awk '{print $1, "\n", $3}' | grep open-ils | uniq -c)

##uptime | awk -F'load average:' '{print "uptime:",$2,"\n"}'
echo "apache2: ${numweb}"; echo "nginx:  ${numnginx}"; echo ""; echo "no children: ${numdrones}"; echo ""
tail /openils/var/log/osrfsys.log | egrep -i "warning|error" | egrep -v "errors_remaining"
