package com.zj.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

//当前工具类可以适用任何JDBC访问的数据库中任何查询语句
public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clz, boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        //创建集合存放结果数据
        ArrayList<T> result = new ArrayList<>();
        //编译sql语句
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //便利结果集，将每行数据转换为T对象并加入集合
        //获取查询的元数据信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            //创建T对象
            T t = clz.newInstance();
            //列遍历,给T对象赋值
            for (int i = 0; i < columnCount; i++) {
                //获取列名与列值
                String columnName = metaData.getColumnName(i + 1);
                Object value = resultSet.getObject(columnName);
                //判断是否需要下划线转换为驼峰
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                }

                //赋值
                BeanUtils.setProperty(t, columnName, value);
            }
            //将T对象放入集合
            result.add(t);
        }
        resultSet.close();
        preparedStatement.close();

        //返回集合
        return result;
    }

    public static void main(String[] args) throws Exception {
        DruidDataSource dataSource = DruidDSUtil.createDataSource();
        DruidPooledConnection connection = dataSource.getConnection();
        List<JSONObject> jsonObjects = queryList(connection, "select * from GMALL2022_REALTIME.DIM_BASE_TRADEMARK", JSONObject.class, false);
        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }
        connection.close();
    }
}
