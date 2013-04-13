package org.apache.hcatalog.hcatmix.load;

/**
 * Author: malakar
 */

import org.apache.hcatalog.hcatmix.load.hadoop.ReduceResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;

/**
 * Holds statistics for load/store tests of a hcatSpecFile
 */
public class LoadTestStatistics {

    private static final Logger LOG = LoggerFactory.getLogger(LoadTestStatistics.class);
    private String fileName;
    private SortedMap<Long, ReduceResult> timeSeries;
    private String chartUrl;

    public LoadTestStatistics(String fileName, SortedMap<Long, ReduceResult> timeSeries) {
        this.fileName = fileName;
        this.timeSeries = timeSeries;
        chartUrl = LoadTestGrapher.getURL(timeSeries);
    }

    public String getChartUrl() {
        return chartUrl;
    }

    public  SortedMap<Long, ReduceResult> getTimeSeries() {
        return timeSeries;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public String toString() {
        return fileName + ":\n" + timeSeries + "\n" + getChartUrl();
    }
}
