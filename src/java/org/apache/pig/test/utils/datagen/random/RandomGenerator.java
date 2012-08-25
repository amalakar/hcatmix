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

package org.apache.pig.test.utils.datagen.random;

import java.util.Map;
import java.util.Random;

public abstract class RandomGenerator {

    protected int avgSize;
    protected boolean hasMapFile; // indicating whether a map file from
    protected Random rand;

    RandomGenerator(int avgSize, Random rand) {
        this.rand = rand;
        this.avgSize = avgSize;
    }

    // random number to the field value is pre-defined
    abstract public int nextInt(Map<Integer, Object> map);
    abstract public long nextLong(Map<Integer, Object> map);
    abstract public float nextFloat(Map<Integer, Object> map);
    abstract public double nextDouble(Map<Integer, Object> map);
    abstract public String nextString(Map<Integer, Object> map);

    public String randomString() {
        int var = (int)((double) avgSize * 0.3);
        StringBuffer sb = new StringBuffer(avgSize + var);
        if (var < 1) var = 1;
        int len = rand.nextInt(2 * var) + avgSize - var;
        for (int i = 0; i < len; i++) {
            int n = rand.nextInt(122 - 65) + 65;
            sb.append(Character.toChars(n));
        }
        return sb.toString();
    }

    public float randomFloat() {
        return rand.nextFloat() * rand.nextInt();
    }

    public double randomDouble() {
        return rand.nextDouble() * rand.nextInt();
    }
}
