package com.curtis.bigdata.realtime.flinkcdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author curtis.cai
 * @desc TODO
 * @date 2022-06-18
 * @email curtis.cai@outlook.com
 * @reference
 */
public class FlinkCDCSimple {

    public static void main(String[] args) {
        // 1. 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop201")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-flink")
                // 如果不指定则消费指定数据库所有表中的数据
                // .tableList("gmall-flink.base_trademark")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        // 3. 打印数据
        dataStreamSource.print();

        // 4. 启动任务
        try {
            env.execute("FlinkCDCSimple");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
