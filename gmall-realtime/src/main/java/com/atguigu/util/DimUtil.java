package com.atguigu.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String key) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        //先查询数据
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + key;
        String dimJsonStr = jedis.get(redisKey);
        if (dimJsonStr != null) {
            //重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还连接
            jedis.close();

            //返回维表数据
            return JSON.parseObject(dimJsonStr);
        }
        //拼接SQL语句
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id ='" + key + "'";
        System.out.println("querySql>>>>>>>>>>>>>>" + querySql);
        //查询
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        System.out.println(queryList);
        //将从phoenix 查询到的数据写入Redis
        JSONObject dimInfo = queryList.get(0);
        jedis.set(redisKey, dimInfo.toJSONString());
        //设置过期时间
        jedis.expire(redisKey, 24 * 60 * 60);
        //归还时间
        jedis.close();
        //返回数据
        return queryList.get(0);
    }
    public static void delDimInfo(String tableName, String key) {
        //获取连接
        Jedis jedis = JedisUtil.getJedis();
        //删除数据
        jedis.del("DIM:" + tableName + ":" +key);
        //归还连接
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();
        long start = System.currentTimeMillis();
        JSONObject dimInfo = getDimInfo(connection, "DIM_BASE_TRADEMARK", "13");
        long end = System.currentTimeMillis();
        System.out.println(dimInfo);
        System.out.println(end - start);
        connection.close();
    }
}
