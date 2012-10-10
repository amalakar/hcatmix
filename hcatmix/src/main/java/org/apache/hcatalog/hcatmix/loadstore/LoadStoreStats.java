/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hcatalog.hcatmix.loadstore;

import com.googlecode.charts4j.*;

import static com.googlecode.charts4j.Color.*;

import org.perf4j.GroupedTimingStatistics;
import org.perf4j.TimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LoadStoreStats {

    private static final Logger LOG = LoggerFactory.getLogger(LoadStoreStats.class);
    private String fileName;
    private GroupedTimingStatistics timedStats;
    private String chartUrl;

    public LoadStoreStats(String fileName, GroupedTimingStatistics timedStats) {
        this.fileName = fileName;
        this.timedStats = timedStats;
        chartUrl = getChartURL(); //TODO remove this
    }

    public String getChartUrl() {
        return chartUrl;
    }

    protected String getChartURL() {
        List<Double> minsList = new ArrayList<Double>();
        List<Double> avgList = new ArrayList<Double>();
        List<Double> maxList = new ArrayList<Double>();

        List<String> labelsReverse = new ArrayList<String>();
        List<String> labels = new ArrayList<String>();
        double max = 0.0;
        for (Map.Entry<String, TimingStatistics> statEntry : timedStats.getStatisticsByTag().entrySet()) {
            labels.add(statEntry.getKey());
            TimingStatistics stat = statEntry.getValue();
            minsList.add((double) stat.getMin());
            avgList.add(stat.getMean());
            maxList.add((double) stat.getMax());
            if (stat.getMax() > max) {
                max = stat.getMax();
            }
            LOG.info(statEntry.getKey() + " " + stat);
        }
        // Reverse the order of labels, as that is what charts4j expects
        for (int i = labels.size() - 1; i >=0 ; i--) {
            labelsReverse.add(labels.get(i));
        }

        final double MAX_LIMIT = max + 0.1 * max;

        Data mins = DataUtil.scaleWithinRange(0, MAX_LIMIT, minsList);
        Data avgs = DataUtil.scaleWithinRange(0, MAX_LIMIT, avgList);
        Data maxes = DataUtil.scaleWithinRange(0, MAX_LIMIT, maxList);
        BarChartPlot maxBar = Plots.newBarChartPlot(maxes, Color.newColor("9ad2ff"), "Max");
        BarChartPlot avgBar = Plots.newBarChartPlot(avgs, Color.newColor("0040ff"), "Avg");
        BarChartPlot minBar = Plots.newBarChartPlot(mins, Color.newColor("6898ce"), "Min");
        BarChart chart = GCharts.newBarChart(maxBar, avgBar, minBar);

        // Defining axis info and styles
        AxisStyle axisStyle = AxisStyle.newAxisStyle(BLACK, 13, AxisTextAlignment.CENTER);
        AxisLabels typeAxisLabels = AxisLabelsFactory.newAxisLabels(labelsReverse);
        typeAxisLabels.setAxisStyle(axisStyle);
        AxisLabels timeAxis = AxisLabelsFactory.newAxisLabels("Time in milliseconds");
        timeAxis.setAxisStyle(axisStyle);
        AxisLabels timeScale = AxisLabelsFactory.newNumericRangeAxisLabels(0, MAX_LIMIT);
        timeScale.setAxisStyle(axisStyle);


        // Adding axis info to chart.
        chart.addXAxisLabels(timeScale);
        chart.addXAxisLabels(timeAxis);
        chart.addYAxisLabels(typeAxisLabels);
        chart.addTopAxisLabels(timeScale);
        chart.setHorizontal(true);
        chart.setSize(650, 450);

        chart.setTitle(fileName, BLACK, 16);
        chart.setGrid((MAX_LIMIT / 500) * 20, 600, 3, 2);
        chartUrl = chart.toURLString();
        LOG.info("Generated Chart: " + chartUrl);
        return  chartUrl;
    }

    public GroupedTimingStatistics getTimedStats() {
        return timedStats;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public String toString() {
        return fileName + ":\n" + timedStats + "\n" + getChartUrl();
    }
}
