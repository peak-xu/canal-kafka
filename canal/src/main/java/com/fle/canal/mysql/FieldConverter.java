package com.fle.canal.mysql;

import com.alibaba.dts.formats.avro.Field;
import org.apache.commons.lang3.StringUtils;

public interface FieldConverter {
    FieldValue convert(Field field, Object o);
    public static FieldConverter getConverter(String sourceName, String sourceVersion) {
        if (StringUtils.endsWithIgnoreCase("mysql", sourceName)) {
            return new MysqlFieldConverter100();
        } else {
            throw new RuntimeException("FieldConverter: only mysql supported for now");
        }
    }
}
