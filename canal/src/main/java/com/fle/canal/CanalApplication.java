package com.fle.canal;

import com.fle.canal.client.CanalClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
@SpringBootApplication
public class CanalApplication {
    protected final static Logger logger = LoggerFactory.getLogger(CanalApplication.class);

    @Bean
    CanalClient canalClient() {
        return new CanalClient();
    }
    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(CanalApplication.class, args);

        CanalClient canalClient = context.getBean(CanalClient.class);
        canalClient.start();
        logger.info("## started the canal client ##");

        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                try {
                    logger.info("## stop  the canal client ##");
                    canalClient.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:##", e);
                } finally {
                    logger.info("## canal client is down ##");
                }
            }

        });
    }

}
