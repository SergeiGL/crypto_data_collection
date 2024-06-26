#!/bin/bash

cat script_list.txt | while read -r line ;
do
./parse.sh $line
done
