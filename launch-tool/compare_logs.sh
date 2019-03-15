#!/bin/bash

## compare_logs.sh


if [ $# -eq 0 ] || [ "$1" -lt 1 ]; then
    echo "usage: compare_logs.sh <num>"
    exit 1
fi

peers=$(($1 - 1))
for i in `seq 0 $peers`
do
    for j in `seq $(($i + 1 )) $peers`
    do
        if ! diff -q logs/peer$i.log.trim logs/peer$j.log.trim &>/dev/null; then
            echo "Logs $i $j Differ!"
            diff logs/peer$i.log.trim logs/peer$j.log.trim
            echo
        else
            echo "Logs $i $j... PASS"
        fi
    done
done
