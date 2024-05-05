#!/bin/bash
go build -buildmode=plugin ../mrapps/wc.go

for ((i=0; i<3000; i++)); do
    rm mr-out*
    bash test-mr.sh > test_lab1.txt
    echo "Iteration number: $i"
done
