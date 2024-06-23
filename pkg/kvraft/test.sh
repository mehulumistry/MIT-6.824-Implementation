log_file="test_results_$(date +%Y%m%d_%H%M%S).log"
log_fileD="test_results3D_$(date +%Y%m%d_%H%M%S).log"

for i in {1..1}; do
    echo "Test iteration $i" >> $log_file

    # Uncomment and use the following lines if you want to run Test 3A with a timeout
    # echo "Running Test 3A" >> $log_file
    # timeout 120s go test -run 3A >> $log_file 2>&1
    # result=$?
    # if [ $result -eq 124 ]; then
    #     echo "Test 3A timed out on iteration $i" >> $log_file
    # elif [ $result -ne 0 ]; then
    #     echo "Test 3A failed on iteration $i" >> $log_file
    #     break
    echo "Running Test 3C" >> $log_file
    timeout 120s go test -run TestUnreliable4A >> $log_file 2>&1
    result=$?
    if [ $result -ne 0 ]; then
        echo "Test 3C failed on iteration $i" >> $log_file
        break
    fi
done