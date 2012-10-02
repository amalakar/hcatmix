package org.apache.pig.test.utils;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNull;

public class TestDataType {

    @Test
    public void testDataType() {
        assertEquals('i', DataType.INT.character());
        assertEquals(DataType.INT, DataType.fromChar('i'));
        assertEquals(DataType.INT, DataType.fromInt(105));
        try {
            DataType.fromChar('x');
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
        }
        assertNull(DataType.fromInt(25));
    }
}
