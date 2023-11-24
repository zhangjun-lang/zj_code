package com.zj.app.func;

import com.zj.util.keywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String text) {
        List<String> list = null;
        try {
            list = keywordUtil.splitKeyword(text);
            for (String keyword : list) {
                collect(Row.of(keyword));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
