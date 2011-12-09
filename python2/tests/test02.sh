TEST=02
rm -fr tmp/cache
python process_test_get.py testfile 0.0 2.0 5.0 CONTENT 5.0 > "tmp/log${TEST}_0.txt" &
python process_test_get.py testfile 0.5 10.0 0.0 > "tmp/log${TEST}_1.txt"

sleep 20
LOG0=`cat tmp/log${TEST}_0.txt`
LOG1=`cat tmp/log${TEST}_1.txt`
REFLOG0=`cat refs/log${TEST}_0.txt`
REFLOG1=`cat refs/log${TEST}_1.txt`

function compare {    
    if [ "$1" == "$2" ];
    then
        echo "OK $3";
    else
        echo "NOK $3: $1 != $2"
    fi
}

compare "$LOG0" "$REFLOG0" "0"
compare "$LOG1" "$REFLOG1" "1"

