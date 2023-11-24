package jun.zhang.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import jun.zhang.bean.TradePaymentWindowBean;
import jun.zhang.util.DateFormatUtil;
import jun.zhang.util.MyClickHouseUtil;
import jun.zhang.util.MyKafkaUtil;
import jun.zhang.util.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
//数据流：Web/App -> nginx -> 业务服务器 -> Maxwell -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD) -> FlinkApp ->kafka(DWD)
// -> FlinkApp -> ClickHouse(DWS)
//程序： Mock -> Mysql -> Maxwell -> kafka(zk) -> DwdTradeOrderPreProcess -> kafka(zk) -> DwdTradeOrderDetail -> kafka(zk) -> DwdTradePayDetailSuc -> kafka(zk)
// -> DwsTradePaymentSucWindow - > ClickHouse(ZK)
public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取dwd层成功支付主题数据创建流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //3.将数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println(">>>>>>>>>>>>>>>" + s);
                }
            }
        });
        //4.按照订单明细id分组
        KeyedStream<JSONObject, String> jsonObjKeyedDetailIdDS = jsonObjDS.keyBy(json -> json.getString("order_detail_id"));
        //5.使用状态编程保留最新的数据输出
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjKeyedDetailIdDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                //获取状态中的数据
                JSONObject state = valueState.value();
                if (state == null) {
                    valueState.update(jsonObject);
                    context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
                } else {
                    String stateRt = state.getString("row_op_ts");
                    String curRt = jsonObject.getString("row_op_ts");
                    int compare = TimestampLtz3CompareUtil.compare(stateRt, curRt);
                    if (compare != 1) {
                        valueState.update(jsonObject);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                //输出并清空状态数据
                JSONObject value = valueState.value();
                out.collect(value);
                valueState.clear();
            }
        });
        //6.提取事件时间生成Watermark

        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        String callback_time = jsonObject.getString("callback_time");
                        return DateFormatUtil.toTs(callback_time, true);
                    }
                }));

        //7.按照user_id分组
        KeyedStream<JSONObject, String> keyedByUidDS = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));
        //8.提取独立支付成功用户数
        SingleOutputStreamOperator<TradePaymentWindowBean> tradePaymentDS = keyedByUidDS.flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
            private ValueState<String> lastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-dt", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TradePaymentWindowBean> collector) throws Exception {
                //取出状态中以及当前数据的日期
                String lastDt = lastDtState.value();
                String curDt = jsonObject.getString("callback_time").split(" ")[0];
                //定义当前支付人数以及新增付费用户数
                Long paymentSucUniqueUserCount = 0L;
                Long paymentSucNewUserCount = 0L;
                //判断状态日期是否为null
                if (lastDt == null) {
                    paymentSucNewUserCount = 1L;
                    paymentSucUniqueUserCount = 1L;
                    lastDtState.update(curDt);
                } else if (!lastDt.equals(curDt)) {
                    paymentSucUniqueUserCount = 1L;
                    lastDtState.update(curDt);
                }
                //返回数据
                if (paymentSucUniqueUserCount == 1L) {
                    collector.collect(new TradePaymentWindowBean("",
                            "",
                            paymentSucUniqueUserCount,
                            paymentSucNewUserCount,
                            null));
                }
            }
        });
        //9.开窗，聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = tradePaymentDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean tradePaymentWindowBean, TradePaymentWindowBean t1) throws Exception {
                        tradePaymentWindowBean.setPaymentSucUniqueUserCount(tradePaymentWindowBean.getPaymentSucUniqueUserCount() + t1.getPaymentSucUniqueUserCount());
                        tradePaymentWindowBean.setPaymentSucNewUserCount(tradePaymentWindowBean.getPaymentSucNewUserCount() + t1.getPaymentSucNewUserCount());

                        return tradePaymentWindowBean;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> iterable, Collector<TradePaymentWindowBean> collector) throws Exception {
                        TradePaymentWindowBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        collector.collect(next);
                    }
                });

        //10.将数据写出到clickhouse
        resultDS.print(">>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));
        //11.启动任务
        env.execute("DwsTradePaymentSucWindow");
    }
}
