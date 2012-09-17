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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestColSpec {

    @Test
    public void testSpringRepresentation() {
        ColSpec colSpec = new ColSpec.Builder()
                .dataType(DataType.fromString("string"))
                .avgStrLength(20)
                .cardinality(160000)
                .distributionType(ColSpec.DistributionType.fromString("zipf"))
                .percentageNull(7)
                .build();
        assertEquals("s:20:160000:z:7", colSpec.getStringRepresentation());

        ColSpec bagColSpec = new ColSpec.Builder()
                .dataType(DataType.fromString("map"))
                .avgStrLength(10)
                .cardinality(1)
                .distributionType(ColSpec.DistributionType.fromString("zipf"))
                .percentageNull(20)
                .build();
        colSpec = new ColSpec.Builder()
                .dataType(DataType.fromString("bag"))
                .bagColSpec(bagColSpec)
                .build();
        assertEquals("bm:10:1:z:20", colSpec.getStringRepresentation());
    }

    public void testStringParsing(){
        ColSpec colSpec = ColSpec.fromString("s:20:160000:z:7");
        assertEquals(DataType.STRING, colSpec.getDataType());
        assertEquals(20, colSpec.getAvgStrLength());
        assertEquals(160000, colSpec.getCardinality());
        assertEquals(ColSpec.DistributionType.ZIPF, colSpec.getDistype());
        assertEquals(7, colSpec.getPercentageNull());
        assertEquals("s:20:160000:z:7", colSpec.getStringRepresentation());
    }
}
