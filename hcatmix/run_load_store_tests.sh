#! /bin/sh

#100GB_300_parititons.xml  100GB_no_partition.xml  10GB_300_partitions.xml  10GB_no_paritions.xml  1GB_600_partition.xml  1GB_no_partition.xml  210MB_600_partitions.xml  210MB_no_partitions.xml  README
# 100GB_600_parititons.xml  100GB_no_partition.xml  10GB_600_partitions.xml  10GB_no_paritions.xml  1GB_600_partition.xml  1GB_no_partition.xml  210MB_600_partitions.xml  210MB_no_partitions.xml  README
function run_test {
    FILE_NAME=$1
    NUM_RUNS=$2

    echo "Running: $FILE_NAME $NUM_RUNS"
    RESOURCE_PATH=./src/main/resources/performance/

    ./src/main/resources/run.sh loadstoretest $RESOURCE_PATH/$FILE_NAME $NUM_RUNS 2>&1 | tee ./log/$FILE_NAME.log
    mkdir -p results
    mv hcatmix_loadstore_results.html results/${FILE_NAME}_${NUM_RUNS}.html
    mv hcatmix_loadstore_results.json results/${FILE_NAME}_${NUM_RUNS}.json
    echo done
}

#run_test 100GB_600_parititons.xml 1
run_test 100GB_no_partition.xml 3
#run_test 100GB_600_parititons.xml 2
#run_test 10GB_300_partitions.xml 1
#run_test 10GB_no_paritions.xml 5
#run_test 1GB_600_partition.xml 5
#run_test 1GB_no_partition.xml 5
#run_test 210MB_600_partitions.xml 5
#run_test 210MB_no_partitions.xml 5
