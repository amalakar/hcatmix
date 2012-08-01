package org.apache.pig.test.utils.datagen;

import org.apache.pig.test.utils.DataType;

/**
 * Author: malakar
 */
public class DataGeneratorTest {
    public static void main(String[] args) {
        testCHar();
    }
    public static void testCHar() {
        DataType type = DataType.INT;
        System.out.println(type.character());

        System.out.print((int)'');

    }

}
