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

public class UniformRandomGenerator extends RandomGenerator {
    int cardinality;

    public UniformRandomGenerator(int avgStrLength, int cardinality, Random rand) {
        super(avgStrLength, rand);
        this.cardinality = cardinality;
    }

    public int nextInt(Map<Integer, Object> map) {
        return rand.nextInt(cardinality);
    }

    public long nextLong(Map<Integer, Object> map) {
        return rand.nextLong() % cardinality;
    }

    public float nextFloat(Map<Integer, Object> map) {
        int seed = rand.nextInt(cardinality);
        Float f = (Float)map.get(seed);
        if (f == null) {
            if (!hasMapFile) {
                f = randomFloat();
                map.put(seed, f);
            }else{
                throw new IllegalStateException("Number " + seed + " is not found in map file");
            }
        }
        return f;
    }

    public double nextDouble(Map<Integer, Object> map) {
        int seed = rand.nextInt(cardinality);
        Double d = (Double)map.get(seed);
        if (d == null) {
            if (!hasMapFile) {
                d = randomDouble();
                map.put(seed, d);
            }else{
                throw new IllegalStateException("Number " + seed + " is not found in map file");
            }
        }
        return d;
    }

    public String nextString(Map<Integer, Object> map) {
        int seed = rand.nextInt(cardinality);
        String s = (String)map.get(seed);
        if (s == null) {
            if (!hasMapFile) {
                s = randomString();
                map.put(seed, s);
            }else{
                throw new IllegalStateException("Number " + seed + " is not found in map file");
            }
        }
        return s;
    }
}
