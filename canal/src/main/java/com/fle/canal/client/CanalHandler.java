package com.fle.canal.client;


import com.alibaba.dts.formats.avro.*;
import com.alibaba.dts.formats.avro.Character;
import com.alibaba.dts.formats.avro.Integer;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fle.canal.producer.HandlerProducer;
import com.fle.canal.producer.MysqlFieldConverter;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xufengfeng on 2019-10-26 下午 4:39.
 * Desc:
 */
@Component
public class CanalHandler {


    protected final static Logger logger = LoggerFactory.getLogger(CanalHandler.class);

    @Autowired
    private HandlerProducer handlerProducer;

    /**
     * 处理canal发送过来的数据
     *
     * @param running   是否继续
     * @param connector 连接canal服务器的连接对象
     */
    protected void handler(boolean running, CanalConnector connector) {
        int batchSize = 5 * 1024;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            while (running) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    emptyCount = 0;
                    printEntry(message.getEntries(), message);
                }
                connector.ack(batchId); // 提交确认
            }
            System.out.println("empty too many times, exit");
        } catch (Exception e) {
            logger.error("accept canal data but handle error!", e);
        } finally {
            connector.disconnect();
        }

    }

    protected void printEntry(List<CanalEntry.Entry> entrys, Message message) throws Exception {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin = null;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id
                    logger.info("BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    CanalEntry.TransactionEnd end = null;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("END ----> transaction id: {}", end.getTransactionId());
                }
                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
                CanalEntry.EventType eventType = rowChage.getEventType();
                if (eventType == CanalEntry.EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info("QuerySql|DdlSql ----> " + rowChage.getSql());

                    //TODO
                    CanalBean canalBean = new CanalBean(entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                            entry.getHeader().getExecuteTime(), eventType.getNumber(), rowChage.getSql());
                    //向kafka发送数据
                    //handlerProducer.sendMessage(canalBean, true);
                    continue;
                }

                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    Source source = new Source();
                    Record record = new Record();

                    if ("mysql".equals(entry.getHeader().getSchemaName())) {
                        record.setVersion(169493228);
                        record.setId(message.getId());
                        record.setSourceTimestamp(entry.getHeader().getExecuteTime());
                        record.setSourcePosition(entry.getHeader().getLogfileOffset() + "@10");
                        record.setSafeSourcePosition(entry.getHeader().getLogfileOffset() + "@10");
                        record.setSourceTxid("0");

                        source.setSourceType(SourceType.MySQL);
                        source.setVersion("5.6.16-log");
                        record.setSource(source);
                        if ("INSERT".equals(entry.getHeader().getEventType().name())) {
                            record.setOperation(Operation.INSERT);
                            setTags(rowData.getAfterColumnsList(),record);
                            setFields(rowData.getAfterColumnsList(),record);
                            setAfterImages(rowData.getAfterColumnsList(),record);
                        } else if ("UPDATE".equals(entry.getHeader().getEventType().name())) {
                            record.setOperation(Operation.UPDATE);
                            setTags(rowData.getAfterColumnsList(),record);
                            setFields(rowData.getAfterColumnsList(),record);
                            setAfterImages(rowData.getAfterColumnsList(),record);
                            setBeforeImages(rowData.getBeforeColumnsList(),record);

                        } else if ("DELETE".equals(entry.getHeader().getEventType().name())) {
                            record.setOperation(Operation.DELETE);
                            setTags(rowData.getBeforeColumnsList(),record);
                            setFields(rowData.getBeforeColumnsList(),record);
                            setBeforeImages(rowData.getBeforeColumnsList(),record);
                        }

                        record.setObjectName(entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName());
                        //record.setProcessTimestamps();默认为null
                        //record.setTags();在printColumnToList赋值
                        //record.setFields();
                    }
                    //向kafka发送数据
                    logger.info("----------source----" + source);
                    logger.info("-----------record-----------" + record);
                    handlerProducer.sendMessage(record, true);
                }
            }
        }
    }

    private void setAfterImages(List<CanalEntry.Column> columns, Record record) throws UnsupportedEncodingException {
        List<Object> list = new ArrayList<>();
        for(CanalEntry.Column column:columns){
            if(column.getMysqlType().contains("int")){
                Integer integer = Integer.newBuilder()
                        .setPrecision(MysqlFieldConverter.INT.enumToInt())
                        .setValue(column.getValue())
                        .build();
                list.add(integer);
            }else if(column.getMysqlType().contains("varchar")){
                Character character = Character.newBuilder()
                        .setCharset("utf8mb4")
                        .setValue(ByteBuffer.wrap(column.getValue().getBytes("ASCII")))
                        .build();
                list.add(character);
            }else{
                Integer integer = Integer.newBuilder()
                        .setPrecision(10000)
                        .setValue(column.getValue())
                        .build();
                list.add(integer);
            }
        }
        record.setAfterImages(list);
    }

    private void setBeforeImages(List<CanalEntry.Column> columns, Record record) throws UnsupportedEncodingException {
        List<Object> list = new ArrayList<>();
        for(CanalEntry.Column column:columns){
            if(column.getMysqlType().contains("int")){
                Integer integer = Integer.newBuilder()
                        .setPrecision(MysqlFieldConverter.INT.enumToInt())
                        .setValue(column.getValue())
                        .build();
                list.add(integer);
            }else if(column.getMysqlType().contains("varchar")){
                Character character = Character.newBuilder()
                        .setCharset("utf8mb4")
                        .setValue(ByteBuffer.wrap(column.getValue().getBytes("ASCII")))
                        .build();
                list.add(character);
            }else{
                Integer integer = Integer.newBuilder()
                        .setPrecision(10000)
                        .setValue(column.getValue())
                        .build();
                list.add(integer);
            }
        }
        record.setBeforeImages(list);
    }


    protected void setTags(List<CanalEntry.Column> columns,Record record) throws Exception {
        Map<CharSequence,CharSequence> map =new HashMap<>();
        for (CanalEntry.Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append("name:" + column.getName() + " + isKey:" + column.getIsKey() + " + updated:" + column.getUpdated() + " + isNull:" + column.getIsNull() + " + value:" + column.getValue());
            logger.info(builder.toString());
            if (column.getIsKey()) {
                map.put("pk_uk_info","{\"PRIMARY\":[\""+column.getName()+"\"]}");
                record.setTags(map);
            }else {
                throw new Exception("表必须设置主键或唯一键!!");
            }
        }
    }

    protected void setFields(List<CanalEntry.Column> columns,Record record){
            List<Object> list = new ArrayList<>();
        for (CanalEntry.Column column : columns) {
            Field field = new Field();
            StringBuilder builder = new StringBuilder();
            builder.append("name:" + column.getName() + " + isKey:" + column.getIsKey() + " + updated:" + column.getUpdated() + " + isNull:" + column.getIsNull() + " + value:" + column.getValue());
            field.setName(column.getName());
            MysqlFieldConverter dataType = MysqlFieldConverter.valueOf(StringUtils.substringBefore(column.getMysqlType(),"(").toUpperCase());
            field.setDataTypeNumber(dataType.enumToInt());
            list.add(field);
        }
        record.setFields(list);
    }

    public static void main(String[] args) {
        System.out.println("{\"PRIMARY\":[\""+"id"+"\"]}");
        System.out.println("INSERT".equals("INSERT"));
    }

}
