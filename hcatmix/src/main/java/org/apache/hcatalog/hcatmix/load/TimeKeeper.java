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

package org.apache.hcatalog.hcatmix.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TimeKeeper {
    private static final Logger LOG = LoggerFactory.getLogger(TimeKeeper.class);
    private final long timeInterval;
    private final long startTime;
    private final long timeoutInMinutes;
    private final long timeoutBufferInMinutes;

    private final long expiryTimeInMillis;
    private final long expiryTimeWithBufferInMillis;
    private long currentCheckPoint = 0;

    public TimeKeeper(final long timeoutInMinutes, final long timeoutBufferInMinutes, final long timeInterval) {
        this(System.currentTimeMillis(), timeoutInMinutes, timeoutBufferInMinutes, timeInterval);
    }

    public TimeKeeper(final long startTime, final long timeoutInMinutes, final long timeoutBufferInMinutes, final long timeInterval) {
        this.timeoutInMinutes = timeoutInMinutes;
        this.timeoutBufferInMinutes = timeoutBufferInMinutes;
        this.timeInterval = timeInterval;
        this.startTime = startTime;

        expiryTimeInMillis = startTime + this.timeoutInMinutes * 60 * 1000;
        expiryTimeWithBufferInMillis = expiryTimeInMillis + timeoutBufferInMinutes * 60 * 1000;
    }

    public TimeKeeper(TimeKeeper timeKeeper) {
        this(timeKeeper.startTime, timeKeeper.timeoutInMinutes, timeKeeper.timeoutBufferInMinutes, timeKeeper.timeInterval);
    }

    public void updateCheckpoint() {
        final long currentTime = currentTimeInMinutes();
        currentCheckPoint = currentTime - (currentTime % timeInterval);
        LOG.info("New checkpoint is :" + currentCheckPoint);
    }

    private static long currentTimeInMinutes() {
        return TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis());
    }

    public boolean hasExpired() {
        return System.currentTimeMillis() >= expiryTimeInMillis;
    }

    public boolean hasExpiredIncludingBuffer() {
        return System.currentTimeMillis() >= expiryTimeWithBufferInMillis;
    }

    public long getRemainingTimeIncludingBuffer() {
        return expiryTimeWithBufferInMillis - System.currentTimeMillis();
    }

    public long getPercentageProgress() {
        return (System.currentTimeMillis() - startTime) / (expiryTimeInMillis -startTime);
    }

    public boolean hasNextCheckpointArrived() {
        return currentTimeInMinutes() >= currentCheckPoint + timeInterval;
    }

    public long getCurrentCheckPoint() {
        return currentCheckPoint;
    }
}
