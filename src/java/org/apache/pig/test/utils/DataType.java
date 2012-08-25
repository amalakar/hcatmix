package org.apache.pig.test.utils;

import java.util.HashMap;
import java.util.Map;

public enum DataType {
    INT('i'),
    LONG('l'),
    FLOAT('f'),
    DOUBLE('d'),
    STRING('s'),
    MAP('m'),
    BAG('b');

    private final char character;
    DataType(char character) {
        this.character = character;
    }

    private static final Map<Character, DataType> charToEnum
            = new HashMap<Character, DataType>();
    static { // Initialize map from constant char to enum constant
        for (DataType dataType : values())
            charToEnum.put(dataType.character(), dataType);
    }

    public static DataType fromChar(char c) {
        DataType dataType = charToEnum.get(c);
        if(dataType == null) {
            throw new IllegalArgumentException("Don't know column type: " + c);
        }
        return dataType;
    }

    public static DataType fromInt(int c) {
        return charToEnum.get((char) c);
    }

    public char character() {
        return character;
    }
}
