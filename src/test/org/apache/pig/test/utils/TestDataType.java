package org.apache.pig.test.utils;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class TestDataType {

    @Test
    public void testDataType() {
        assertEquals('i', DataType.INT.character());
        assertEquals(DataType.INT, DataType.fromChar('i'));
        assertEquals(DataType.INT, DataType.fromInt(105));
        assertNull(DataType.fromChar('x'));
        assertNull(DataType.fromInt(25));
    }

}
