-- This script covers having a nested plan with splits.
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader() as (user, action, timespent, query_term,
            ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, timestamp;
C = group B by user;
D = foreach C {
    morning = filter B by timestamp < 43200;
    afternoon = filter B by timestamp >= 43200;
    generate group, COUNT(morning), COUNT(afternoon);
}
store D into '$output/L7out';
