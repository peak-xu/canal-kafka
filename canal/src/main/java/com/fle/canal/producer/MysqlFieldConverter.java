package com.fle.canal.producer;

/**
 * Created by xufengfeng on 2019-10-31 下午 7:50.
 * Desc:
 */
public enum MysqlFieldConverter {
    DECIMAL(0),INT8(1),INT16(2),INT32(3),Float(4),DOUBLE(5),
    NULL(6),TIMESTAMP(7),INT64(8),INT24(9),DATE(10),TIME(11),
    DATETIME(12),INT(300),VARCHAR(301);

    private int value;

    private MysqlFieldConverter(int value){
        this.value=value;
    }

    public static MysqlFieldConverter intToEnum(int value) {    //将数值转换成枚举值
        switch (value) {
            case 0:
                return DECIMAL;
            case 4:
                return Float;
            case 5:
                return DOUBLE;
            case 10:
                return DATE;
            case 12:
                return DATETIME;
            case 300:
                return INT;
            case 301:
                return VARCHAR;
            default :
                return null;
        }
    }
    public int enumToInt() { //将枚举值转换成数值
        return this.value;
    }

}
