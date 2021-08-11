import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.dis.FlinkDisConsumer;

import com.huaweicloud.dis.adapter.common.consumer.DisConsumerConfig;

public class FlinkDIS
{
    public static void main(String[] args)
        throws Exception
    {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = getTestProperties();
        DataStream<String> stream = env
            .addSource(
                new FlinkDisConsumer<>((String)properties.get("stream.name"), new SimpleStringSchema(), properties))
            .setParallelism(1);
        stream.map(new MapFunction<String, String>()
        {
            private static final long serialVersionUID = -6867736771747690202L;
            
            @Override
            public String map(String value)
                throws Exception
            {
                return "Stream Value: " + value;
            }
        }).print();
        env.execute();
    }
    
    public static Properties getTestProperties()
    {
        Properties properties = new Properties();
        properties.setProperty("ak", "H682EOBNFN2PILLZN931");
        properties.setProperty("sk", "PY5TAAgWo0NRXNIzkH0eJrraEdYI5LKUsgOnFtaN");
        properties.setProperty("region", "cn-south-1");
        properties.setProperty("projectId", "061701e1728026af2f59c00f87e00125");
        properties.setProperty("endpoint", "https://dis.cn-south-1.myhuaweicloud.com");
        properties.setProperty("group.id", "flink_consumer");
        properties.setProperty("flink.partition-discovery.interval-millis", "10000");
        properties.setProperty("stream.name", "dis-tracker");
        properties.setProperty(DisConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000");
        properties.setProperty("flink.poll-timeout", "100");
        return properties;
    }
}
