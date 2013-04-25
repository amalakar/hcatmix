package org.apache.hcatalog.hcatmix.load;

import org.apache.hcatalog.hcatmix.publisher.ResultsPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: malakar
 */
public class LoadTestAllResults {
    private Map<String, LoadTestStatistics> results;
    private static final Logger LOG = LoggerFactory.getLogger(LoadTestAllResults.class);
    private final String htmlOutFileName;
    private final String jsonOutFileName;

    public LoadTestAllResults(final String htmlOutFileName, final String jsonOutFileName) {
        this.htmlOutFileName = htmlOutFileName;
        this.jsonOutFileName = jsonOutFileName;
        results = new HashMap<String, LoadTestStatistics>();
    }

    public void addResult(LoadTestStatistics stats) {
        results.put(stats.getFileName(), stats);
    }

    public void publish() throws Exception {
        for (Map.Entry<String, LoadTestStatistics> hCatStatsEntry : results.entrySet()) {
            String fileName = hCatStatsEntry.getKey();
            LoadTestStatistics stats = hCatStatsEntry.getValue();
            LOG.info(fileName + " Statistics:\n" + stats +
                    "\nChart URL: " + stats.getChartUrl());
        }
        ResultsPublisher publisher = new LoadTestResultsPublisher(new ArrayList<LoadTestStatistics>(results.values()),
                htmlOutFileName, jsonOutFileName);
        publisher.publishAll();
    }

    public Map<String, LoadTestStatistics> getResults() {
        return results;
    }
}
