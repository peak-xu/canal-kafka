package com.fle.canal.client;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;


import java.net.InetSocketAddress;

/**
 * Created by xufengfeng on 2019-10-26 下午 3:52.
 * Desc:
 */

public class CanalClient {
    protected final static Logger logger = LoggerFactory.getLogger(CanalClient.class);

    @Value("${canal.destination}")
    private String destination;

    @Value("${canal.server.ip}")
    private String ip;

    @Value("${canal.server.username}")
    private String username;

    @Value("${canal.server.password}")
    private String password;

    protected Thread thread = null;

    protected volatile boolean running = true;

    @Autowired
    private CanalHandler canalHandler;

    protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("parse events has an error", e);
        }
    };

    public void start() {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111), destination, username, password);
        canalHandler.handler(running, connector);
//        thread = new Thread(new Runnable() {
//            public void run() {
//            }
//        });
//        thread.setUncaughtExceptionHandler(handler);
//        thread.start();
//        running = false;
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        MDC.remove("destination");
    }


}