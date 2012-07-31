-- This script covers multiple distinct aggregates
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, action, estimated_revenue, timespent;
C = group B by user;
D = foreach C {
    beth = distinct B.action;
    rev = distinct B.estimated_revenue;
    ts = distinct B.timespent;
    generate group, COUNT(beth), SUM(rev), (int)AVG(ts);
}
store D into '$output/L15out';

