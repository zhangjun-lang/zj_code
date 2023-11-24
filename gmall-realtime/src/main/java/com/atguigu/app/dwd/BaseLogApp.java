package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.MyKafkaUtil;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD)
//程序： Mock(lg.sh)     - >   Flume(f1) -> kafka(zk)  -> BaseLogApp -> kafka(zk)
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.消费kafka topic_log 主题数据
        String topic = "topic_log";
        String groupId = "base_log_app_211126";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //3.过滤掉非JSON格式对象&将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }

            }
        });
        //获取测输出流脏数据
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>>>>>");
        //4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //5.使用状态编程做新老访客标记检验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //获取is_new & ts
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                //将时间戳转化为年月日
                String curDate = DateFormatUtil.toDate(ts);
                //获取状态中的日期
                String lastDate = lastVisitState.value();
                //判断is_new标记是否为1
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));

                }
                return jsonObject;
            }
        });
        //6.使用测输出流进行数据分流处理,页面主流,启动，曝光，动作，错误测输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                //尝试获取错错误信息
                String err = jsonObject.getString("err");
                if (err != null) {
                    //将数据写到error侧数据流
                    context.output(errorTag, jsonObject.toJSONString());
                }
                jsonObject.remove("err");
                String start = jsonObject.getString("start");
                if (start != null) {
                    //将数据写到start侧数据流
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    //获取公告信息&页面id&时间戳
                    String common = jsonObject.getString("common");
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    Long ts = jsonObject.getLong("ts");
                    //获取曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //遍历曝光数据写出到display侧输出流
                        for (int i = 0; i < displays.size(); ++i) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                    //获取动作数据
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        //遍历曝光数据写出到display侧输出流
                        for (int i = 0; i < actions.size(); ++i) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            context.output(actionTag, action.toJSONString());
                        }
                    }
                    //移除曝光和动作数据& 写到页面日志主流
                    jsonObject.remove("display");
                    jsonObject.remove("actions");
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });
        //7.提取各个测输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        //8.将数据打印并写入对应的主题
        pageDS.print("Page>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>>");
        actionDS.print("Action>>>>>>>>>>>");
        errorDS.print("Error>>>>>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));
        //9.启动任务
        env.execute("BaseLogApp");
    }
}
