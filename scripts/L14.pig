-- This script covers merge join
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_page_views_sorted' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, estimated_revenue;
alpha = load '$input/pigmix_users_sorted' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
beta = foreach alpha generate name;
C = join B by user, beta by name using 'merge';
store C into '$output/L14out';

