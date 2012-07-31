--This script covers order by of multiple values.
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent:int, query_term, ip_addr, timestamp,
        estimated_revenue:double, page_info, page_links);
B = order A by query_term, estimated_revenue desc, timespent;
store B into '$output/L10out';
