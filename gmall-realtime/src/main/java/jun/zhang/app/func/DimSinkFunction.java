package jun.zhang.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import jun.zhang.util.DimUtil;
import jun.zhang.util.DruidDSUtil;
import jun.zhang.util.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private static DruidDataSource druidDataSource = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();

    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        //获取连接
        DruidPooledConnection connection = druidDataSource.getConnection();
        String sinkTable = value.getString("sinkTable");
        JSONObject data = value.getJSONObject("data");
        //获取数据类型
        String type = value.getString("type");
        //如果为更新数据，则需要删除redis数据
        if ("update".equals(type)) {
            DimUtil.delDimInfo(sinkTable.toUpperCase(), data.getString("id"));
        }

        //写出数据


        PhoenixUtil.upsertValues(connection, sinkTable, data);


        //归还连接
        connection.close();
    }
}
