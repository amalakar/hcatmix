REGISTER /home/y/libexec/hive/lib/*.jar
REGISTER /home/y/libexec/hive/lib/hive-metastore-0.9.1.0.jar
REGISTER /home/y/libexec/hive/conf/hive-site.xml

in = load '/user/malakar/hcatmix_uniform_bug/page_views_20000000_0/part-00000' USING PigStorage(',') AS (user:chararray, timespent:int, query_term:chararray, ip_addr:int, estimated_revenue:int, page_info:chararray, action:int);

STORE in into 'page_views_20000000_0' USING org.apache.hcatalog.pig.HCatStorer();
