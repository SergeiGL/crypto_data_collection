#!/bin/bash

cat script_list.txt | while read -r line ;
do
stringarray=($line)
sudo docker logs --tail 10 ${stringarray[0]}
done
