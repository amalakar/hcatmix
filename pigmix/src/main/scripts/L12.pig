-- This script covers multi-store queries.
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user, action, (int)timespent as timespent, query_term,
    (double)estimated_revenue as estimated_revenue;
split B into C if user is not null, alpha if user is null;
split C into D if query_term is not null, aleph if query_term is null;
E = group D by user;
F = foreach E generate group, MAX(D.estimated_revenue);
store F into '$output/L12out/highest_value_page_per_user';
beta = group alpha by query_term;
gamma = foreach beta generate group, SUM(alpha.timespent);
store gamma into '$output/L12out/total_timespent_per_term';
beth = group aleph by action;
gimel = foreach beth generate group, COUNT(aleph);
store gimel into '$output/L12out/queries_per_action';
