#!/usr/bin/env bash

# Show usage if we didn't get 3 arguments
if [ $# != 3 ]; then
  echo "Usage: $0 num_maps num_rows output_dir"
  echo ""
  echo "   num_maps:   The number of map tasks to use to generate data"
  echo "   num_rows:   The number of rows to output (100000 ~ 1GB)"
  echo "   output_dir: The output directory on the DFS"
  echo ""
  exit 1
fi

# Get the input parameters
NUM_MAPS=$1
NUM_ROWS=$2
OUTPUT_DIR=$3
hadoop_opts="" #"-Dmapred.job.queue.name=unfunded"
pig_opts="-x local"


java=${JAVA_HOME:='/usr'}/bin/java;
if [ ! -e $java ] 
then
    echo "Cannot find java, set JAVA_HOME environment variable to directory above bin/java."
    exit
fi

if [ ! -e $PIG_HOME ] 
then
    echo "Cannot find PIG_HOME, set PIG_HOME environment variable to directory above bin/pig."
    exit
fi

if [ ! -e $HADOOP_HOME ]
then
    echo "Cannot find HADOOP_HOME, set PIG_HOME environment variable to directory above bin/hadoop."
    exit
fi

pigjar=$PIG_HOME/lib/pig.jar
if [ ! -e $pigjar ]
then
    echo "Cannot find $pigjar, not in $PIG_HOME"
    exit
fi

scripts=`dirname "$0"`
scripts=`cd $scripts; pwd`
pigperfjar=$scripts/../pigperf.jar

if [ ! -e $pigperfjar ]
then
    echo "Cannot find $pigperfjar, run ant to build it"
    exit
fi

# configure the cluster
conf_dir="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"
export HADOOP_CLASSPATH=$pigjar:$pigperfjar:$scripts/../lib/sdsuLibJKD12.jar

START=$(date +%s)

# Prepare the data generation
mainclass=org.apache.pig.test.utils.datagen.DataGenerator
hadoop_common_args="--config $conf_dir jar $pigperfjar $mainclass $hadoop_opts -s ,"

user_field=s:20:160000:z:7
action_field=i:1:2:u:0
os_field=i:1:20:z:0
query_term_field=s:10:180000:z:20
ip_addr_field=l:1:100000:z:0
timestamp_field=l:1:86400:z:0
estimated_revenue_field=d:1:100000:z:5
page_info_field=m:10:1:z:0
page_links_field=bm:10:1:z:20

page_views="$OUTPUT_DIR/pigmix_page_views"
echo "+++++++++++++++++++++++++"
echo "Generating $page_views"

$HADOOP_HOME/bin/hadoop $hadoop_common_args \
    -m $NUM_MAPS -r $NUM_ROWS -f $page_views $user_field \
    $action_field $os_field $query_term_field $ip_addr_field \
    $timestamp_field $estimated_revenue_field $page_info_field \
    $page_links_field

# Skim off 1 in 10 records for the user table
# Be careful the file is in HDFS if you run previous job as hadoop job,
# you should either copy data into local disk to run following script
# or run hadoop job to trim the data

protousers="$OUTPUT_DIR/pigmix_protousers"
echo "+++++++++++++++++++++++++"
echo "Skimming users"

$PIG_HOME/bin/pig $hadoop_opts $pig_opts << EOF
register $pigperfjar;
fs -rmr $protousers;
A = load '$page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel $NUM_MAPS;
D = order C by \$0 parallel $NUM_MAPS;
store D into '$protousers';
EOF


# Create users table, with new user field.
phone_field=s:10:160000:z:20
address_field=s:20:160000:z:20
city_field=s:10:160000:z:20
state_field=s:2:1600:z:20
zip_field=i:2:1600:z:20

users="$OUTPUT_DIR/pigmix_users"
echo "+++++++++++++++++++++++++"
echo "Generating $users"

$HADOOP_HOME/bin/hadoop $hadoop_common_args \
    -m $NUM_MAPS -i $protousers -f $users $phone_field \
    $address_field $city_field $state_field $zip_field

# Find unique keys for fragment replicate join testing
# If file is in HDFS, extra steps are required
numuniquekeys=500
protopowerusers="$OUTPUT_DIR/pigmix_protopower_users"
echo "+++++++++++++++++++++++++"
echo "Skimming power users"

$PIG_HOME/bin/pig $hadoop_opts $pig_opts << EOF
register $pigperfjar;
fs -rmr $protopowerusers;
A = load '$page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B parallel $NUM_MAPS;
D = order C by \$0 parallel $NUM_MAPS;
E = limit D 500;
store E into '$protopowerusers';
EOF


powerusers="$OUTPUT_DIR/pigmix_power_users"
echo "+++++++++++++++++++++++++"
echo "Generating $powerusers"

$HADOOP_HOME/bin/hadoop $hadoop_common_args \
    -m $NUM_MAPS -i $protopowerusers -f $powerusers $phone_field \
    $address_field $city_field $state_field $zip_field


widerows="$OUTPUT_DIR/pigmix_widerow"
widegroupbydata="$OUTPUT_DIR/pigmix_widegroupbydata"
widerowcnt=$NUM_ROWS
user_field=s:20:10000:z:0
int_field=i:1:10000:u:0 
echo "+++++++++++++++++++++++++"
echo "Generating widerow"

$HADOOP_HOME/bin/hadoop $hadoop_common_args \
    -m $NUM_MAPS -r $widerowcnt -f $widerows $user_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field \
    $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field $int_field

$PIG_HOME/bin/pig $hadoop_opts $pig_opts << EOF
register $pigperfjar;
fs -rmr ${page_views}_sorted;
fs -rmr ${users}_sorted;
fs -rmr ${powerusers}_samples;
A = load '$page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = order A by user parallel $NUM_MAPS;
store B into '${page_views}_sorted' using PigStorage('\u0001');

alpha = load '$users' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
a1 = order alpha by name parallel $NUM_MAPS;
store a1 into '${users}_sorted' using PigStorage('\u0001');

a = load '$powerusers' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
b = sample a 0.5;
store b into '${powerusers}_samples' using PigStorage('\u0001');

A = load '$page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links,
user as user1, action as action1, timespent as timespent1, query_term as query_term1, ip_addr as ip_addr1, timestamp as timestamp1, estimated_revenue as estimated_revenue1, page_info as page_info1, page_links as page_links1,
user as user2, action as action2, timespent as timespent2, query_term as query_term2, ip_addr as ip_addr2, timestamp as timestamp2, estimated_revenue as estimated_revenue2, page_info as page_info2, page_links as page_links2;
store B into '$widegroupbydata' using PigStorage('\u0001');
EOF

mkdir -p pigmix_power_users
$PIG_HOME/bin/pig $hadoop_opts << EOF
fs -copyToLocal ${powerusers}/* pigmix_power_users;
EOF

cat pigmix_power_users/* > local_power_users

# Cleanup
$PIG_HOME/bin/pig $hadoop_opts $pig_opts<< EOF
fs -rmr $protousers;
fs -rmr $protopowerusers;
fs -rmr $powerusers;
fs -copyFromLocal local_power_users $OUTPUT_DIR/pigmix_power_users;
EOF

rm -rf pigmix_power_users
rm local_power_users

# Done
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Total data generation time (seconds): $DIFF"

