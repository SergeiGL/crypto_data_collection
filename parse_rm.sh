#!/bin/bash

cat script_list.txt | while read -r line ;
do
stringarray=($line)
sudo docker rm -f ${stringarray[0]}
done
