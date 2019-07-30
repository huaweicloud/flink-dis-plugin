package org.apache.flink.streaming.dis.example;

import com.huaweicloud.dis.DISConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.dis.FlinkDisProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DISFlinkStreamingSinkJavaExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(DISFlinkStreamingSinkJavaExample.class);

    public static void main(String[] args) {
        LOGGER.info("Start DIS Flink Streaming Sink Java Demo.");

        // DIS终端节点，如 https://dis.cn-north-1.myhuaweicloud.com
        String endpoint;
        // DIS服务所在区域ID，如 cn-north-1
        String region;
        // 用户的AK
        String ak;
        // 用户的SK
        String sk;
        // 用户的项目ID
        String projectId;
        // DIS通道名称
        String streamName;

        if (args.length < 6) {
            LOGGER.error("args is wrong, should be [endpoint region ak sk projectId streamName]");
            return;
        }

        endpoint = args[0];
        region = args[1];
        ak = args[2];
        sk = args[3];
        projectId = args[4];
        streamName = args[5];

        try {
            // get an ExecutionEnvironment
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment();

            // Read from text file
            DataStream<String> stream = env.readTextFile("huaweicloud-dis-flink-connector-demo/src/main/resources/text");

            stream.print();

            // DIS Config
            DISConfig disConfig = DISConfig.buildDefaultConfig();
            disConfig.put(DISConfig.PROPERTY_ENDPOINT, endpoint);
            disConfig.put(DISConfig.PROPERTY_REGION_ID, region);
            disConfig.put(DISConfig.PROPERTY_AK, ak);
            disConfig.put(DISConfig.PROPERTY_SK, sk);
            disConfig.put(DISConfig.PROPERTY_PROJECT_ID, projectId);

            // Write to DIS
            stream.addSink(new FlinkDisProducer<String>(streamName, new SimpleStringSchema(), disConfig));

            env.execute("FlinkWriteToDIS");
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }
}
