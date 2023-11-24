package com.atguigu.app.func;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradeUserSpuOrderBean;
import com.atguigu.util.DimUtil;
import com.atguigu.util.DruidDSUtil;
import com.atguigu.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements  DimJoinFunction<T> {
    private DruidDataSource dataSource = null;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction() {
    }

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = DruidDSUtil.createDataSource();
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }


    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(new Runnable() {
            @Override
            public void run() {

                try {
                    DruidPooledConnection connection = dataSource.getConnection();
                    //查询维表获取维度信息
                    String key = getKey(input);
                    //System.out.println(key);
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);
                    //将维度信息补充至当前位置
                    if (dimInfo != null) {
                        join(input, dimInfo);
                    }


                    connection.close();
                    //结果写出
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("关联维表失败：" + input + "table:" + tableName);
                    //resultFuture.complete(Collections.singletonList(t));
                }
            }
        });

    }




    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut: " + input);
    }
}
