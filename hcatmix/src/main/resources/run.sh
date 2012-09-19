#! /bin/sh

export CLASSPATH=

export CLASSPATH=$CLASSPATH:`find $HIVE_HOME/lib | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $HIVE_HOME/conf | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $HADOOP_HOME/ -name '*.jar' | paste -d ':' -s -`
#export CLASSPATH=$CLASSPATH:`find $HADOOP_HOME/lib | paste -d ':' -s -`
#export CLASSPATH=$CLASSPATH:`find $HADOOP_HOME/conf | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $PIG_HOME/lib/ -name '*.jar' | paste -d ':' -s -`
#export CLASSPATH=$CLASSPATH:`find $PIG_HOME/conf | paste -d ':' -s -`
#export CLASSPATH=$CLASSPATH:`find $PWD/lib/ -name '*.jar' | paste -d ':' -s -`

conf_dir="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"
scripts=`dirname "$0"`
scripts=`cd $scripts/../../../; pwd`
hcatmixjar=$scripts/target/hcatmix-1.0-SNAPSHOT-jar-with-dependencies.jar

export HADOOP_CLASSPATH=$pigperfjar:$PWD/lib/sdsuLibJKD12.jar:$CLASSPATH
hadoop  --config $conf_dir jar $hcatmixjar org.apache.hcatalog.hcatmix.HiveTableCreator -f src/main/resources/hcat_table_specification.xml -m 1 -o /tmp/data -s -p /tmp/pig
#hadoop  org.apache.hcatalog.hcatmix.HiveTableCreator -f scripts/hcat_table_specification.xml -m 0 -o /tmp/hcatmix/ 
