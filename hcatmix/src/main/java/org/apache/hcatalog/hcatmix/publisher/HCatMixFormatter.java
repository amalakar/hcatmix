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

package org.apache.hcatalog.hcatmix.publisher;

import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

/**
 * Util class for formatting number/time duration etc
 */
public class HCatMixFormatter {
    protected PeriodFormatter formatter;

    public HCatMixFormatter() {
        formatter = new PeriodFormatterBuilder()
                .appendHours()
                .appendSuffix("h")
                .appendMinutes()
                .appendSuffix("m")
                .appendSecondsWithMillis()
                .appendSuffix("s")
                .toFormatter();
    }

    public String formatDuration(Double duration) {
        if(duration == null) {
            return "-";
        }

        Period period = new Period(Math.round(duration));
        return formatter.print(period);
    }

    public String formatDuration(Long duration) {
        return  formatDuration((double) duration);
    }

    public String formatDuration(Integer duration) {
        return  formatDuration((double) duration);
    }

    public String formatCount(Integer count) {
        if(count == null) {
            return "0";
        }
        return String.valueOf(count);
    }
}
