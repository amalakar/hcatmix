HCatMix is the performance testing framework for apache HCatalog.
=================================================================

The objective of HCatMix is:

1. Establish the baseline performance numbers, in order to monitor change in performance as new releases are made
2. Find out overhead of using HCatalog
3. Understand limitations of HCatalog (e.g.: number of parallel connections that can be handled by HCatalog Server, number of partitions that can be added in a hadoop job) etc.

It also includes a load generation and testing tool built on top of Hadoop.
See for [more details][https://cwiki.apache.org/confluence/display/HCATALOG/HCatMix].
