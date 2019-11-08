## 项目初始化
* 准备 `mysql`,`canal`, `kafka`,`zookeeper` 环境并启动服务
* 正式环境修改 `/resources/application.properties`参数为`spring.profiles.active=pro`,测试环境为`spring.profiles.active=tra`
* 参数配置 
  ```
  #============== canal ===================
  canal.destination=example      #canal服务端实例
  canal.server.ip=192.168.8.128  #canal服务端ip
  canal.server.username=canal    #canal默认账号
  canal.server.password=canal    #canal默认密码
  
  #============== kafka ===================
  kafka.producer.servers=192.168.8.128:9092  #kafka producer地址端口
  kafka.producer.retries=3                   #重试次数
  kafka.producer.batch.size=4096             #批次大小
  kafka.producer.linger=1                    #等待批次时间
  kafka.producer.buffer.memory=40960         #内存缓冲区大小
  ```

## 项目部署
   运行`mvn clean package -DskipTests`命令打成jar包
   上传至服务器运行 `java -jar ***.jar`