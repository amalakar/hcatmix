#! /bin/sh

export CLASSPATH=

export CLASSPATH=$CLASSPATH:`find $HIVE_HOME/lib | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $HIVE_HOME/conf | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $HADOOP_HOME/ -name '*.jar' | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $HADOOP_HOME/lib | paste -d ':' -s -`
export CLASSPATH=$CLASSPATH:`find $HADOOP_HOME/conf | paste -d ':' -s -`

export HADOOP_CLASSPATH=pigperf.jar:$CLASSPATH
hadoop  org.apache.hcatalog.hcatmix.HiveTableCreator scripts/hcat_table_specification.xml
