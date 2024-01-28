#!/bin/bash  

for((i=1;i<=20;i++));
do
        go test
        echo "finished loop cnt: $i"    
done
