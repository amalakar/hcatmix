-- This script covers wide key group
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_widegroupbydata' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links, user_1, action_1, timespent_1, query_term_1, ip_addr_1, timestamp_1,
        estimated_revenue_1, page_info_1, page_links_1, user_2, action_2, timespent_2, query_term_2, ip_addr_2, timestamp_2,
        estimated_revenue_2, page_info_2, page_links_2);
B = group A by (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, user_1, action_1, timespent_1, query_term_1, ip_addr_1, timestamp_1,
        estimated_revenue_1, user_2, action_2, timespent_2, query_term_2, ip_addr_2, timestamp_2,
        estimated_revenue_2);
C = foreach B generate SUM(A.timespent), SUM(A.timespent_1), SUM(A.timespent_2), AVG(A.estimated_revenue), AVG(A.estimated_revenue_1), AVG(A.estimated_revenue_2);
store C into '$output/L17out';
