package com.fle.canal.producer;

import com.alibaba.dts.formats.avro.Record;
import com.alibaba.dts.formats.avro.Source;
import com.fle.canal.client.CanalBean;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by xufengfeng on 2019-10-26 下午 4:44.
 * Desc:
 */
@Component
public class HandlerProducer {
    private Logger logger = LoggerFactory.getLogger(HandlerProducer.class);

    @Autowired
    private KafkaProducerTask kafkaProducerTask;

    /**
     * 多线程同步提交
     *
     * @param record    canal传递过来的bean
     * @param waiting   是否等待线程执行完成 true:可以及时看到结果; false:让线程继续执行，并跳出此方法返回调用方主程序;
     */
    public void sendMessage(Record record, boolean waiting) {
//        String canalBeanJsonStr = JSON.toJSONString(canalBean);
        Future<String> f = kafkaProducerTask.sendKafkaMessage(String.valueOf(record.getObjectName()), record);
        logger.info("HandlerProducer日志--->当前线程:" + Thread.currentThread().getName() + ",接受的record:" + record);
        if (waiting) {
            try {
                f.get();
            } catch (InterruptedException e) {
                logger.error("Send kafka message job thread pool await termination time out.", e);
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

}
