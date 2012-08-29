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

package org.apache.pig.test.utils.datagen;

import org.apache.pig.test.utils.DataType;
import org.apache.pig.test.utils.datagen.random.RandomGenerator;
import org.apache.pig.test.utils.datagen.random.UniformRandomGenerator;
import org.apache.pig.test.utils.datagen.random.ZipfRandomGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ColSpec {

    private DataType dataType;
    private int averageSize; // TODO: Only applicable in case of strings, should be avgLength
    private int cardinality;
    private DistributionType distype;
    private int percentageNull;
    private String mapFile;
    private ColSpec bagColSpec = null;
    public final static String SEPARATOR = ":";

    private RandomGenerator gen;
    private Map<Integer, Object> map;

    public static enum DistributionType {
        UNIFORM('u'),
        ZIPF('z');
        private final char character;
        DistributionType(char character) {
            this.character = character;
        }

        public char toChar() {
            return character;
        }

        public static DistributionType fromChar(char c) {
            if(c == 'u') {
                return UNIFORM;
            } else if(c == 'z') {
                return ZIPF;
            } else {
                throw new IllegalArgumentException("Unknown char [" + c + "] for distribution type");
            }
        }

        public static DistributionType fromString(String str) {
            if("uniform".equals(str)) {
                return UNIFORM;
            } else if("zipf".equals(str)) {
                return ZIPF;
            } else {
                throw new IllegalArgumentException("Unknown string [" + str + "] for distribution type");
            }
        }
    }

    public static class Builder {
        private DataType dataType;
        private int averageSize;
        private int cardinality;
        private DistributionType distributionType;
        private int percentageNull;
        private String mapFile;
        private ColSpec bagColSpec;
        private Map<Integer, Object> map = new HashMap<Integer, Object>();

        public Builder dataType(DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder averageSize(int averageSize) {
            this.averageSize = averageSize;
            return this;
        }

        public Builder cardinality(int cardinality) {
            this.cardinality = cardinality;
            return this;
        }

        public Builder distributionType(DistributionType distributionType) {
            this.distributionType = distributionType;
            return this;
        }

        public Builder mapFile(String mapFile) {
            this.mapFile = mapFile;
            return this;
        }

        public Builder percentageNull(int percentageNull){
            if (percentageNull < 0 || percentageNull > 100) {
                throw new IllegalArgumentException("Percentage null must be between 0-100, "
                        + "you gave" + percentageNull);
            }
            this.percentageNull = percentageNull;
            return this;
        }

        public Builder bagColSpec(ColSpec bagColSpec) {
            this.bagColSpec = bagColSpec;
            return this;
        }

        public Builder map(Map<Integer, Object> map) {
            this.map = map;
            return this;
        }

        public ColSpec build() {
            return new ColSpec(this);
        }
    }

    public Map<Integer, Object> getMap() {
        return map;
    }

    public ColSpec getBagColSpec() {
        return bagColSpec;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getMapFile() {
        return mapFile;
    }

    public int getAverageSize() {
        return averageSize;
    }

    public int getCardinality() {
        return cardinality;
    }

    public DistributionType getDistype() {
        return distype;
    }

    public int getPercentageNull() {
        return percentageNull;
    }

    public String getStringRepresentation() {
        StringBuilder args = new StringBuilder(dataType.character()+"");
        if(getDataType() == DataType.BAG) {
            return args.append(bagColSpec.getStringRepresentation()).toString();
        }
        args.append(SEPARATOR);
        args.append(getAverageSize()).append(SEPARATOR).append(cardinality)
                    .append(SEPARATOR).append(distype.toChar()).append(SEPARATOR)
                   .append(percentageNull);
        if(mapFile != null) {
            args.append(SEPARATOR).append(mapFile);
        }

        return args.toString();
    }

    public RandomGenerator getGen() {
        return gen;
    }

    private ColSpec(Builder builder) {
        this.dataType = builder.dataType;
        this.averageSize = builder.averageSize;
        this.cardinality = builder.cardinality;
        this.distype = builder.distributionType;
        this.percentageNull = builder.percentageNull;
        this.bagColSpec = builder.bagColSpec;
        this.mapFile = builder.mapFile;
        this.map = builder.map;
        if(builder.distributionType == DistributionType.ZIPF) {
            // TODO
            gen = new ZipfRandomGenerator(builder.averageSize, builder.cardinality, new Random());
        } else {
            gen = new UniformRandomGenerator(builder.averageSize, builder.cardinality, new Random());
        }
    }

    public static ColSpec fromString(String arg) {
        // colspec: columntype:average_size:cardinality:distribution_type:percent_null
        // s:20:160000:z:7
        String[] parts = arg.split(SEPARATOR);
        if (parts.length != 5 && parts.length != 6) {
            throw new IllegalArgumentException("Colspec [" + arg + "] format incorrect");
        }

        Builder builder = new Builder();

        DataType dataType = DataType.fromChar(parts[0].charAt(0));
        builder.dataType(dataType);
        if(dataType == DataType.BAG) {
            // bm:10:1:z:20
            builder.bagColSpec(ColSpec.fromString(arg.substring(1)));
            return builder.build();
        }

        builder.averageSize(Integer.valueOf(parts[1]));
        builder.cardinality(Integer.valueOf(parts[2]));
        builder.distributionType(DistributionType.fromChar(parts[3].charAt(0)));
        builder.percentageNull(Integer.valueOf(parts[4]));

        // if config has 6 columns, the last col is the file name
        // of the mapping file from random number to field value
        if (parts.length == 6) {
            builder.mapFile(parts[5]);
        }

        return builder.build();
    }

    public int nextInt() {
        return gen.nextInt(map);
    }

    public long nextLong() {
        return gen.nextInt(map);
    }

    public double nextDouble() {
        return gen.nextDouble(map);
    }

    public float nextFloat() {
        return gen.nextFloat(map);
    }

    public String nextString() {
        return gen.nextString(map);
    }

    @Override
    public String toString() {
        return getStringRepresentation();
    }
}

