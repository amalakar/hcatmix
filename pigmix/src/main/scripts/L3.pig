--This script tests a join too large for fragment and replicate.  It also 
--contains a join followed by a group by on the same key, something that we
--could potentially optimize by not regrouping.
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, (double)estimated_revenue;
alpha = load '$input/pigmix_users' using PigStorage('\u0001') as (name, phone, address,
        city, state, zip);
beta = foreach alpha generate name;
C = join beta by name, B by user;
D = group C by $0;
E = foreach D generate group, SUM(C.estimated_revenue);
store E into '$output/L3out';

