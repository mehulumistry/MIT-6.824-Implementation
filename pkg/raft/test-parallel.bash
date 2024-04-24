#!/bin/bash

echo "Enter the number of runs:"
read totalRuns

echo "Enter the name of the test to run:"
read testName

echo "Enter the level of parallelism:"
read parallelism

echo "Do you want to save output to files? (yes/no):"
read saveOutput

count=0
if [[ "$saveOutput" == "yes" ]]; then
    rm -f *.txt
fi

for ((i=0; i<$totalRuns; i+=$parallelism))
do
    echo "$count/$totalRuns"

    for ((j=1; j<=$parallelism; j++))
    do {
        num=$[i+j]
        if [[ "$saveOutput" == "yes" ]]; then
            filename="res-$num.txt"
            go test -run $testName -race &> $filename
        else
            go test -run $testName -race
        fi

        if [[ "$saveOutput" == "yes" ]]; then
            current=$(grep -o 'ok' $filename | wc -l)
            if [ $current -gt 0 ]; then
                echo "($num) test $testName passed once"
            else
                echo "($num) !!!error happened when running test $testName!!!"
                newFilename="error-$num.txt"
                mv $filename $newFilename
            fi
        fi
    } &
    done
    wait

    count=$(($count + $parallelism))
done

if [[ "$saveOutput" == "yes" ]]; then
    echo "$testName tests finished: $count/$totalRuns"
    failed=$(ls error*.txt | wc -l)
    echo "test failed: $failed/$totalRuns"
else
    echo "$testName tests finished."
fi