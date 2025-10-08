sudo tc qdisc add dev jun1.20 root handle 1: htb default 1
sudo tc class add dev jun1.20 parent 1: classid 1:1 htb rate 500mbit
