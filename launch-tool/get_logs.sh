#!/bin/bash

## get_logs.sh

if [ $# -eq 0 ] || [ "$1" -lt 1 ]; then
    echo "usage: get_logs.sh <num>"
    exit 1
fi

peers=$(($1 - 1))
for i in `seq 0 $peers`
do
    echo "writing log peer$i"
    if ! [[ -d "logs" ]]; then
        mkdir "logs"
    fi
    kubectl logs peer$i > logs/peer$i.log
    awk "END{print}" logs/peer$i.log > logs/peer$i.log.trim

done