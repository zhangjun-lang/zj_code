package jun.zhang.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import jun.zhang.bean.UserRegisterBean;
import jun.zhang.util.DateFormatUtil;
import jun.zhang.util.MyClickHouseUtil;
import jun.zhang.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
//数据流：Web/App -> nginx -> 业务服务器 -> Maxwell -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> ClickHouse
//程序： Mock -> Mysql -> Maxwell -> kafka(zk) -> DwdUserRegister -> kafka(zk) -> DwsUserUserRegisterWindow -> clickhouse(zk)
public class DwsUserUserRegisterWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取kafkaDWD层用户注册主题数据创建流
        String topic = "dwd_user_register";
        String groupId = "dws_user_user_register_window";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //3.将每行数据转换为JavaBean对象
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = kafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            String create_time = jsonObject.getString("create_time");
            return new UserRegisterBean("", "", 1L, DateFormatUtil.toTs(create_time, true));
        });
        //4.提取时间戳生成Watermark
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWmDS = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
                    @Override
                    public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                        return userRegisterBean.getTs();
                    }
                }));
        //5.开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> resultDS = userRegisterWithWmDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean userRegisterBean, UserRegisterBean t1) throws Exception {
                        userRegisterBean.setRegisterCt((userRegisterBean.getRegisterCt() + t1.getRegisterCt()));
                        return userRegisterBean;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                        UserRegisterBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        collector.collect(next);
                    }
                });
        //6.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_register_window values(?,?,?,?)"));
        //7.启动任务
        env.execute("DwsUserUserRegisterWindow");
    }
}
