#! /bin/sh

export CLASSPATH=

export CLASSPATH=$CLASSPATH:`find $HIVE_HOME/lib | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $HIVE_HOME/conf | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $HADOOP_HOME/ -name '*.jar' | paste -d ':' -s -`
#export CLASSPATH=$CLASSPATH:`find $HADOOP_HOME/lib | paste -d ':' -s -`
#export CLASSPATH=$CLASSPATH:`find $HADOOP_HOME/conf | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $PIG_HOME/lib/ -name '*.jar' | paste -d ':' -s -`
#export CLASSPATH=$CLASSPATH:`find $PIG_HOME/conf | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $PWD/lib/ -name '*.jar' | paste -d ':' -s -`

conf_dir="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"
scripts=`dirname "$0"`
scripts=`cd $scripts; pwd`
pigperfjar=$scripts/pigperf.jar

export HADOOP_CLASSPATH=$pigperfjar:$PWD/lib/sdsuLibJKD12.jar:$CLASSPATH
hadoop  --config $conf_dir jar $pigperfjar  org.apache.hcatalog.hcatmix.HiveTableCreator -libjars $PWD/lib/sdsuLibJKD12.jar -f scripts/hcat_table_specification.xml -m 0 -o /user/malakar/hcatmix/ 
#hadoop  org.apache.hcatalog.hcatmix.HiveTableCreator -f scripts/hcat_table_specification.xml -m 0 -o /tmp/hcatmix/ 
