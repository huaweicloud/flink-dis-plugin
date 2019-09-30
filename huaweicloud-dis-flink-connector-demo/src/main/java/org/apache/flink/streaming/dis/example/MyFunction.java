package org.apache.flink.streaming.dis.example;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyFunction extends ProcessFunction<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyFunction.class);

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        // TODO
        LOGGER.info("Event Time: {}, Value: {}", ctx.timestamp(), value);
    }
}
