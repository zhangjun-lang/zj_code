package jun.zhang.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import jun.zhang.common.GmallConfig;
import org.apache.commons.lang.StringUtils;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {

    public static void upsertValues(DruidPooledConnection connection, String sinkTable, JSONObject data) throws SQLException {
        //拼接sql: upsert into db.tn(id, name, sex) values ('1001', 'zhangsan', male')
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";
        //预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //执行
        preparedStatement.execute();
        connection.commit();
        //释放资源
        preparedStatement.close();
    }
}
