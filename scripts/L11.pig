-- This script covers distinct and union.
SET default_parallel $factor

register ../pigperf.jar;
A = load '$input/pigmix_page_views' using org.apache.pig.test.udf.storefunc.PigPerformanceLoader()
    as (user, action, timespent, query_term, ip_addr, timestamp,
        estimated_revenue, page_info, page_links);
B = foreach A generate user;
C = distinct B;
alpha = load '$input/pigmix_widerow' using PigStorage('\u0001');
beta = foreach alpha generate $0 as name;
gamma = distinct beta;
D = union C, gamma;
E = distinct D;
store E into '$output/L11out';
