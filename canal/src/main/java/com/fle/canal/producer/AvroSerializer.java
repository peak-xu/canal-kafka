package com.fle.canal.producer;

import com.alibaba.dts.formats.avro.Record;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import com.fle.canal.client.CanalHandler;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xufengfeng on 2019-10-28 下午 2:43.
 * Desc:
 */
public class AvroSerializer implements Serializer<Record>{

    protected final static Logger logger = LoggerFactory.getLogger(AvroSerializer.class);
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Record record) {
        if(record == null) {
            return null;
        }
        DatumWriter<Record> writer = new SpecificDatumWriter<Record>(Record.SCHEMA$);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        try {
            writer.write(record, encoder);
        }catch (IOException e) {
            throw new SerializationException(e.getMessage());
        }
        return out.toByteArray();
    }



    @Override
    public void close() {

    }
}
