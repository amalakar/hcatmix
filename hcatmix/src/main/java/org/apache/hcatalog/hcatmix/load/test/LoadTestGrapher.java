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

package org.apache.hcatalog.hcatmix.load.test;

import com.googlecode.charts4j.*;
import org.apache.hcatalog.hcatmix.load.hadoop.ReduceResult;
import org.perf4j.TimingStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.googlecode.charts4j.Color.*;

/**
 * Author: malakar
 */
public class LoadTestGrapher {
    private static final Logger LOG = LoggerFactory.getLogger(LoadTestGrapher.class);

    public static String getURL(SortedMap<Long, ReduceResult> results) {
        // Defining lines
        Map<String, List<Double>> timeSeries = new HashMap<String, List<Double>>();

        final List<String> xAxisTimeLabels = new ArrayList<String>();
        final List<String> xAxisThreadCountLabels = new ArrayList<String>();

        double max = 0;
        for (Map.Entry<Long, ReduceResult> entry : results.entrySet()) {
            Long timeStamp = entry.getKey();
            ReduceResult result = entry.getValue();
            for (Map.Entry<String, TimingStatistics> statisticsByTag : result.getStatistics().getStatisticsByTag().entrySet()) {
                String taskName = statisticsByTag.getKey();
                Double avg = statisticsByTag.getValue().getMean();
                if(timeSeries.containsKey(taskName)) {
                    timeSeries.get(taskName).add(avg);
                } else {
                    List<Double> avgs = new ArrayList<Double>();
                    avgs.add(avg);
                    timeSeries.put(taskName, avgs);
                }
                max = avg > max ? avg : max;
            }
            xAxisTimeLabels.add(timeStamp.toString());
            xAxisThreadCountLabels.add(String.valueOf(result.getThreadCount()));
        }
        final double MAX_LIMIT = max + 0.1 * max;

        List<Line> lines = new ArrayList<Line>();
        for (Map.Entry<String, List<Double>> taskTimeSeries : timeSeries.entrySet()) {
            Line avgTime = Plots.newLine(DataUtil.scaleWithinRange(0, MAX_LIMIT, taskTimeSeries.getValue()), Color.newColor("CA3D05"), taskTimeSeries.getKey());
            avgTime.setLineStyle(LineStyle.newLineStyle(3, 1, 0));
            avgTime.addShapeMarkers(Shape.DIAMOND, Color.newColor("CA3D05"), 12);
            avgTime.addShapeMarkers(Shape.DIAMOND, Color.WHITE, 8);
            lines.add(avgTime);
        }

        // Defining chart.
        LineChart chart = GCharts.newLineChart(lines);
        chart.setSize(600, 450);
        chart.setTitle("Response Time VS Response Time (in Milliseconds)", WHITE, 14);
        chart.setGrid(25, 25, 3, 2);

        // Defining axis info and styles
        AxisStyle axisStyle = AxisStyle.newAxisStyle(WHITE, 12, AxisTextAlignment.CENTER);
        AxisLabels timestampAxis = AxisLabelsFactory.newAxisLabels(xAxisTimeLabels);
        timestampAxis.setAxisStyle(axisStyle);
        AxisLabels threadCountAxis = AxisLabelsFactory.newAxisLabels(xAxisThreadCountLabels);
        threadCountAxis.setAxisStyle(axisStyle);
        AxisLabels threadCountLabel = AxisLabelsFactory.newAxisLabels("Thread Count", 50.0);
        threadCountLabel.setAxisStyle(AxisStyle.newAxisStyle(WHITE, 14, AxisTextAlignment.CENTER));

        AxisLabels timeScale = AxisLabelsFactory.newNumericRangeAxisLabels(0, MAX_LIMIT);
        timeScale.setAxisStyle(axisStyle);

        // Adding axis info to chart.
        chart.addXAxisLabels(timestampAxis);
        chart.addXAxisLabels(threadCountAxis);
        chart.addXAxisLabels(threadCountLabel);
        chart.addYAxisLabels(timeScale);

        // Defining background and chart fills.
        chart.setBackgroundFill(Fills.newSolidFill(Color.newColor("1F1D1D")));
        LinearGradientFill fill = Fills.newLinearGradientFill(0, Color.newColor("363433"), 100);
        fill.addColorAndOffset(Color.newColor("2E2B2A"), 0);
        chart.setAreaFill(fill);
        String url = chart.toURLString();
        LOG.info(url);
        return url;
    }
}
