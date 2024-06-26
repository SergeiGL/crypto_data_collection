#!/bin/bash

cat script_list.txt | while read -r line ;
do
stringarray=($line)
sudo docker start ${stringarray[0]}
done
