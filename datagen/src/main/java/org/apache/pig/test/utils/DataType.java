package org.apache.pig.test.utils;

import java.util.HashMap;
import java.util.Map;

public enum DataType {
    INT('i', "int"),
    LONG('l', "long"),
    FLOAT('f', "float"),
    DOUBLE('d', "double"),
    STRING('s', "string"),
    MAP('m', "map"),
    BAG('b', "bag");

    private final char character;
    private final String string;
    DataType(char character, String string) {
        this.character = character;
        this.string = string;
    }

    private static final Map<Character, DataType> charToEnum
            = new HashMap<Character, DataType>();
    static { // Initialize map from constant char to enum constant
        for (DataType dataType : values())
            charToEnum.put(dataType.character(), dataType);
    }

    private static final Map<String, DataType> strToEnum
            = new HashMap<String, DataType>();
    static { // Initialize map from constant char to enum constant
        for (DataType dataType : values())
            strToEnum.put(dataType.toString(), dataType);
    }

    public static DataType fromChar(char c) {
        DataType dataType = charToEnum.get(c);
        if(dataType == null) {
            throw new IllegalArgumentException("Don't know column type: " + c);
        }
        return dataType;
    }

    public static DataType fromString(String str) {
        if(str != null) {
            str = str.toLowerCase();
        }
        DataType dataType = strToEnum.get(str);
        if(dataType == null) {
            throw new IllegalArgumentException("Don't know column type: " + str);
        }
        return dataType;
    }

    public static DataType fromInt(int c) {
        return charToEnum.get((char) c);
    }

    public char character() {
        return character;
    }

    public String toString() {
        return string;
    }


}
