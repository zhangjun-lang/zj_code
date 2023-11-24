package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD)
//程序： Mock(lg.sh)     - >   Flume(f1) -> kafka(zk)  -> BaseLogApp -> kafka(zk) -> DwdTrafficUserJumpDetail -> kafka(zk)

public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取kafka页面日志主题数据
        String topic = "dwd_traffic_page_log";
        String groupId = "dwd_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //3.将数据转换成JSON数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //4.提取事件事件&按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }))
                .keyBy(json -> json.getJSONObject("common").getString("mid"));
        //5.定义CEP的模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));
        //6.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
        //7.提取事件
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut"){

        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                }, new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                });
        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);
        //8.合并数据
        DataStream<String> unionDS = selectDS.union(timeOutDS);
        //9.打印数据
        selectDS.print(">>>>>>>>>>>>>>");
        timeOutDS.print(">>>>>>>>>>>>>>>>>>");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));
        //10.启动任务
        env.execute("DwdTrafficUserJumpDetail");
    }
}
