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
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

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

                    if ("MYSQL".equals(entry.getHeader().getSourceType().name())) {
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
                            setTags(rowData.getAfterColumnsList(), record);
                            setFields(rowData.getAfterColumnsList(), record);
                            record.setAfterImages(setImages(rowData.getAfterColumnsList()));
                        } else if ("UPDATE".equals(entry.getHeader().getEventType().name())) {
                            record.setOperation(Operation.UPDATE);
                            setTags(rowData.getAfterColumnsList(), record);
                            setFields(rowData.getAfterColumnsList(), record);
                            record.setAfterImages(setImages(rowData.getAfterColumnsList()));

                            record.setBeforeImages(setImages(rowData.getBeforeColumnsList()));
                        } else if ("DELETE".equals(entry.getHeader().getEventType().name())) {
                            record.setOperation(Operation.DELETE);
                            setTags(rowData.getBeforeColumnsList(), record);
                            setFields(rowData.getBeforeColumnsList(), record);
                            record.setBeforeImages(setImages(rowData.getBeforeColumnsList()));

                        }

                        record.setObjectName(entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName());
                        //record.setProcessTimestamps();默认为null
                        //record.setTags();在printColumnToList赋值
                        //向kafka发送数据
                        logger.info("----------source----" + source);
                        //record.setFields();
                    }
                    logger.info("-----------record-----------" + record);
                    handlerProducer.sendMessage(record, true);
                }
            }
        }
    }

    private List<Object> setImages(List<CanalEntry.Column> columns) throws UnsupportedEncodingException, ParseException {
        List<Object> list = new ArrayList<>();
        for (CanalEntry.Column column : columns) {
            String value = column.getValue();
            if (Objects.isNull(value)) {
                list.add(new Object());
                continue;
            }

            if (StringUtils.indexOfIgnoreCase(column.getMysqlType(), "int") != -1) {
                String mysqlType = column.getMysqlType().toUpperCase();
                logger.info("--------------mysqlType------------"+mysqlType);
                if(com.mysql.jdbc.StringUtils.indexOfIgnoreCase(mysqlType,"UNSIGNED")!=-1){
                    mysqlType=StringUtils.substringBefore(mysqlType, "UNSIGNED");
                }
                MysqlFieldConverter dataType = MysqlFieldConverter.valueOf(StringUtils.substringBefore(mysqlType, "("));

                Integer integer = Integer.newBuilder()
                        .setPrecision(dataType.enumToInt())
                        .setValue(column.getValue())
                        .build();
                list.add(integer);
            } else if (StringUtils.startsWithIgnoreCase(column.getMysqlType(), "varchar")) {

                Character character = Character.newBuilder()
                        .setCharset("utf8mb4")
                        .setValue(ByteBuffer.wrap(column.getValue().getBytes("ASCII")))
                        .build();
                list.add(character);
            } else if (StringUtils.startsWithIgnoreCase(column.getMysqlType(), "datetime")) {
                if(!column.getValue().isEmpty()){
                    String dateStr = column.getValue();
                    Date date = new SimpleDateFormat("yyyyMMddHHmmssSSS").parse(dateStr);
                    SimpleDateFormat date0 = new SimpleDateFormat("yyyy");
                    SimpleDateFormat date1 = new SimpleDateFormat("MM");
                    SimpleDateFormat date2 = new SimpleDateFormat("dd");
                    SimpleDateFormat date3 = new SimpleDateFormat("HH");
                    SimpleDateFormat date4 = new SimpleDateFormat("mm");
                    SimpleDateFormat date5 = new SimpleDateFormat("ss");
                    SimpleDateFormat date6 = new SimpleDateFormat("SSS");

                    DateTime dateTime = DateTime.newBuilder()
                            .setYear(java.lang.Integer.valueOf(date0.format(date)))
                            .setMonth(java.lang.Integer.valueOf(date1.format(date)))
                            .setDay(java.lang.Integer.valueOf(date2.format(date)))
                            .setHour(java.lang.Integer.valueOf(date3.format(date)))
                            .setMinute(java.lang.Integer.valueOf(date4.format(date)))
                            .setSecond(java.lang.Integer.valueOf(date5.format(date)))
                            .setMillis(java.lang.Integer.valueOf(date6.format(date)))
                            .build();

                    list.add(dateTime);
                }
            } else {
                Character character = Character.newBuilder()
                        .setCharset("utf8mb4")
                        .setValue(ByteBuffer.wrap(column.getValue().getBytes("ASCII")))
                        .build();
                list.add(character);
            }
        }
        return list;
    }


    private void setTags(List<CanalEntry.Column> columns, Record record) throws Exception {
        Map<CharSequence, CharSequence> map = new HashMap<>();
        for (CanalEntry.Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append("name:" + column.getName() + " + isKey:" + column.getIsKey() + " + updated:" + column.getUpdated() + " + isNull:" + column.getIsNull() + " + value:" + column.getValue());
            logger.info(builder.toString());
            if (column.getIsKey()) {
                map.put("pk_uk_info", "{\"PRIMARY\":[\"" + column.getName() + "\"]}");
                record.setTags(map);
            }else{
                record.setTags(map);
            }
        }
    }

    private void setFields(List<CanalEntry.Column> columns, Record record) {

        List<Object> list = new ArrayList<>();
        for (CanalEntry.Column column : columns) {
            Field field = new Field();
            StringBuilder builder = new StringBuilder();
            builder.append("name:" + column.getName() + " + isKey:" + column.getIsKey() + " + updated:" + column.getUpdated() + " + isNull:" + column.getIsNull() + " + value:" + column.getValue());
            field.setName(column.getName());
            String mysqlType = column.getMysqlType().toUpperCase();
            if(com.mysql.jdbc.StringUtils.indexOfIgnoreCase(mysqlType,"UNSIGNED")!=-1){
                mysqlType=StringUtils.substringBefore(mysqlType, "UNSIGNED");
            }
            MysqlFieldConverter dataType = MysqlFieldConverter.valueOf(StringUtils.substringBefore(mysqlType, "("));
            field.setDataTypeNumber(dataType.enumToInt());
            list.add(field);
        }
        record.setFields(list);
    }

    public static void main(String[] args) throws SQLException {
        System.out.println("{\"PRIMARY\":[\"" + "id" + "\"]}");
        System.out.println("INSERT".equals("INSERT"));
    }

}
