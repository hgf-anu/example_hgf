package com.yaxin.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FFReducer2 extends Reducer<Text, Text, Text, Text> {

    private Text v = new Text();
    private StringBuilder sb = new StringBuilder();

    /**
     * 传到Reducer2的数据格式如下
     * A-B	 C
     * A-B	 E
     * 把value用逗号拼起来即可
     *
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        sb.delete(0,sb.length());
        for (Text value : values) {
            sb.append(value.toString()).append(",");
        }
        v.set(sb.toString());
        context.write(key,v);
    }
}
