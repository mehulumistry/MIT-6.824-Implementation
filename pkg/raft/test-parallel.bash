#!/bin/bash

echo "Enter the number of runs:"
read totalRuns

echo "Enter the name of the test to run:"
read testName

echo "Enter the level of parallelism:"
read parallelism

echo "Do you want to use the race detector? (yes/no):"
read useRace

raceFlag=""
if [[ "$useRace" == "yes" ]]; then
    raceFlag="-race"
fi

count=0

for ((i=0; i<$totalRuns; i+=$parallelism))
do
    echo "$count/$totalRuns"
    echo -n "Progress: ["
    for ((p=0; p<$totalRuns; p+=$parallelism)); do
        if [ $p -le $count ]; then
            echo -n "#"
        else
            echo -n "-"
        fi
    done
    echo "]"

    for ((j=1; j<=$parallelism; j++))
    do {
        num=$[i+j]
        filename="output-$num.txt"
        go test -run $testName $raceFlag &> $filename
        current=$(grep -o 'ok' $filename | wc -l)
        if [ $current -gt 0 ]; then
            echo "($num) test $testName passed once"
            rm $filename  # Remove the file as the test passed
        else
            echo "($num) !!!error happened when running test $testName!!!"
            mv $filename "error-$num.txt"  # Rename to indicate an error
        fi
    } &
    done
    wait

    count=$(($count + $parallelism))
done

echo "$testName tests finished: $count/$totalRuns"
failed=$(ls error-*.txt 2>/dev/null | wc -l)
echo "test failed: $failed/$totalRuns"