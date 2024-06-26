#!/bin/bash

cat script_list.txt | while read -r line ;
do
stringarray=($line)
sudo docker stop ${stringarray[0]}
done
