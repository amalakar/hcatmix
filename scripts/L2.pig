-- This script tests using a join small enough to do in fragment and replicate. 
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, estimated_revenue;
alpha = load '$input/pigmix_power_users' using PigStorage('\u0001') as (name, phone,
        address, city, state, zip);
beta = foreach alpha generate name;
C = join B by user, beta by name using 'replicated';
D = foreach C generate user, estimated_revenue;
store D into '$output/L2out';
