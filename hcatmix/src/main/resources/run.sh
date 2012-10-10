#! /bin/sh

function get_jar_list {
    SEPARATOR=$1
    export CLASSPATH=
    export CLASSPATH=${CLASSPATH}${SEPARATOR}`find $HIVE_HOME/lib -type f -name "*.jar"| paste -d $SEPARATOR -s -`
    export CLASSPATH=${CLASSPATH}${SEPARATOR}`find $HIVE_HOME/conf | paste -d $SEPARATOR -s -`
    export CLASSPATH=${CLASSPATH}${SEPARATOR}`find $HADOOP_HOME/ -type f -name '*.jar' | paste -d $SEPARATOR -s -`
    export CLASSPATH=${CLASSPATH}${SEPARATOR}`find $PIG_HOME/lib/ -type f -name '*.jar' | paste -d $SEPARATOR -s -`
    echo $CLASSPATH
}
conf_dir="${HADOOP_CONF_DIR:-$HADOOP_HOME/conf}"
scripts=`dirname "$0"`
scripts=`cd $scripts/../../../; pwd`
hcatmixjar=$scripts/target/hcatmix-1.0-SNAPSHOT-jar-with-dependencies.jar

export HADOOP_CLASSPATH=`get_jar_list :`
export JAR_LIST=`get_jar_list ,`

export action=$1
case $action in
    listPartition)
        echo "Executing HCatListPartitionTask"
        hadoop  --config $conf_dir jar $hcatmixjar org.apache.hcatalog.hcatmix.load.HadoopLoadGenerator \
                -libjars $JAR_LIST --classnames 'org.apache.hcatalog.hcatmix.load.tasks.HCatListPartitionTask'
        ;;
    addPartition)
        echo "Executing HCatAddPartitionTask"
        hadoop  --config $conf_dir jar $hcatmixjar org.apache.hcatalog.hcatmix.load.HadoopLoadGenerator \
                -libjars $JAR_LIST --classnames 'org.apache.hcatalog.hcatmix.load.tasks.HCatAddPartitionTask'
        ;;
    loadtest)
        echo "Executing LoadTestRunner"
        hadoop  --config $conf_dir jar $hcatmixjar org.apache.hcatalog.hcatmix.load.test.LoadTestRunner -libjars $JAR_LIST
        ;;
    loadstoretest)
        echo "Running HCatMixSetup load store tests"
        if [ -n "$2" ]; then
            hcatSpecFile=$2
        fi

        if [ -n "$3" ]; then
            numTimes=$3
        fi
        hadoop  --config $conf_dir jar $hcatmixjar org.apache.hcatalog.hcatmix.loadstore.test.LoadStoreTestRunner \
                -libjars $JAR_LIST -h $hcatSpecFile -n $numTimes
#        hadoop  org.apache.hcatalog.hcatmix.HCatMixSetup -f scripts/hcat_table_specification.xml -m 0 -o /tmp/hcatmix/
        ;;
    *)
        echo "Please provide one of the options"
esac
