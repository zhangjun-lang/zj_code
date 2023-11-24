package com.zj.util;

import com.zj.bean.TransientSink;
import com.zj.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {
    public  static <T> SinkFunction<T> getSinkFunction(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //使用反射的方式获取t对象中的数据
                        Class<?> tClz = t.getClass();
                        Field[] declaredFields = tClz.getDeclaredFields();
                        int offset = 0;
                        //遍历属性
                        for (int i = 0; i < declaredFields.length; ++i) {
                            Field declaredField = declaredFields[i];
                            declaredField.setAccessible(true);
                            //尝试获取注解
                            TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                continue;
                            }
                            //获取属性值
                            Object value = declaredField.get(t);
                            //给站位符赋值
                            preparedStatement.setObject(i + 1 - offset, value);
                        }

                    }
                },new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withPassword("000000")
                        .build());
    }
}
