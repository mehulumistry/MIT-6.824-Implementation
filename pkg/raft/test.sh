for i in {1..50}; do
    echo "Test iteration $i"
    go test -run 3A
    if [ $? -ne 0 ]; then
        echo "Test failed on iteration $i"
        break
    fi
done