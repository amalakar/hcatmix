#!/usr/bin/env bash

# Show usage if we go less than 3 arguments
if [ $# -lt 3 ]; then
  echo "Usage: $0 input_dir output_dir reducers [num_runs [exec_mode]]"
  echo ""
  echo "   input_dir:  The pigmix input directory on HDFS"
  echo "   output_dir: The output directory on the DFS"
  echo "   reducers:   The number of reduce tasks to use per MapReduce job"
  echo "   num_runs:   The number of repetitions for each script (def is 1)"
  echo "   exec_mode:  0 to run just Pig, 1 to run just MR, 2 to run both (def is 0)"
  echo ""
  exit 1
fi

hadoop_opts="" #"-Dmapred.job.queue.name=unfunded"

# Get the input parameters
INPUT_DIR=$1
OUTPUT_DIR=$2
REDUCERS=$3
NUM_RUNS=$4
EXEC_MODE=$5

if [ "$NUM_RUNS" = "" ]; then
   NUM_RUNS=1
fi

if [ "$EXEC_MODE" = "" ]; then
   EXEC_MODE=0
fi

# Check input arguments
if [ "$EXEC_MODE" != "0" ] && [ "$EXEC_MODE" != "1" ] && [ "$EXEC_MODE" != "2" ]; then
    echo "Only valid values for execution mode are 0, 1, or 2."
    exit
fi

# Check environment variables
if [ "$PIG_HOME" == "" ]; then
    echo "Cannot find PIG_HOME, set PIG_HOME environment variable to directory above bin/pig."
    exit
fi

if [ "$HADOOP_HOME" == "" ]; then
    echo "Cannot find HADOOP_HOME, set HADOOP_HOME environment variable to directory above bin/hadoop."
    exit
fi

# Find and check the jar file
scripts=`dirname "$0"`
scripts=`cd $scripts; pwd`
pigperfjar=$scripts/../pigperf.jar

if [ ! -e $pigperfjar ]
then
    echo "Cannot find $pigperfjar, run ant to build it"
    exit
fi

###########################################################
# EXECUTE THE BENCHMARK
cd $scripts
declare cmd=""
declare total_pig_times=0
declare total_mr_times=0

for ((  i = 1 ;  i <= 17;  i++  ))
do
   declare pig_times=0
   declare mr_times=0
   declare multiplier=0
   
   if [ "$EXEC_MODE" == "0" ] || [ "$EXEC_MODE" == "2" ]; then
      # Run each Pig script NUM_RUNS times
      for ((  j = 1 ;  j <= NUM_RUNS;  j++  ))
      do
         # Cleanup
         ${PIG_HOME}/bin/pig $hadoop_opts -e rmf "$OUTPUT_DIR/L${i}out"
         echo ""

         # Prepare the run
         cmd="${PIG_HOME}/bin/pig $hadoop_opts -param input=$INPUT_DIR -param output=$OUTPUT_DIR -param factor=$REDUCERS -f L$i.pig"
         echo "Running Pig Query L$i"
         echo "Going to run $cmd"
      
         # Run the pig script      
         start=$(date +%s)
         $cmd
         end=$(date +%s)
         pig_times=$(( $pig_times + $end - $start ))
      done
   
      # Calculate the average timings
      pig_times=$(( $pig_times / $NUM_RUNS ))
      total_pig_times=$(( $total_pig_times + $pig_times ))
   fi
   
   if [ "$EXEC_MODE" == "1" ] || [ "$EXEC_MODE" == "2" ]; then
      # Run each Hadoop job NUM_RUNS times
      for ((  j = 1 ;  j <= NUM_RUNS;  j++  ))
      do
         # Cleanup
         ${PIG_HOME}/bin/pig $hadoop_opts -e rmf "$OUTPUT_DIR/L${i}out"
         echo ""
      
         # Prepare the run
         cmd="${HADOOP_HOME}/bin/hadoop jar $pigperfjar org.apache.pig.test.pigmix.mapreduce.L$i  $hadoop_opts $INPUT_DIR $OUTPUT_DIR $REDUCERS"
         echo "Running MapReduce Query L$i"
         echo "Going to run $cmd"
      
         # Run the Hadoop job
         start=$(date +%s)
         $cmd
         end=$(date +%s)
         mr_times=$(( $mr_times + $end - $start ))
      done
   
      # Calculate the average timings
      mr_times=$(( $mr_times / $NUM_RUNS ))
      total_mr_times=$(( $total_mr_times + $mr_times ))
   fi
   
   # Print out the result   
   if [ "$EXEC_MODE" == "0" ]; then
      echo "L$i times (sec):	Pig	$pig_times"
   elif [ "$EXEC_MODE" == "1" ]; then
      echo "L$i times (sec):	Java	$mr_times"
   else
      multiplier=`echo "scale=2; $pig_times / $mr_times" | bc`
      echo "L$i times (sec):	Pig	$pig_times	Java	$mr_times	Multiplier	$multiplier"
   fi
   echo ""

done

# Done
if [ "$EXEC_MODE" == "0" ]; then
   echo "Total times (sec):	Pig	$total_pig_times"
elif [ "$EXEC_MODE" == "1" ]; then
   echo "Total times (sec):	Java	$total_mr_times"
else
   multiplier=`echo "scale=2; $total_pig_times / $total_mr_times" | bc`
   echo "Total times (sec):	Pig	$total_pig_times	Java	$total_mr_times	Multiplier	$multiplier"
fi
echo ""


