-- This script covers outer join
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
	as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
B = foreach A generate user, estimated_revenue;
alpha = load '$input/pigmix_power_users_samples' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
beta = foreach alpha generate name, phone;
C = join B by user left outer, beta by name;
store C into '$output/L13out';

