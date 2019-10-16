package com.yaxin.friend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FFReduce1 extends Reducer<Text, Text, Text, Text> {
    private Text v = new Text();
    private StringBuilder sb = new StringBuilder();

    /**
     * 中间结果转换为:
     * A	I,K,C,B,G,F,H,O,D,
     * B	A,F,J,E,
     * C	A,E,B,H,F,G,K,
     * D	G,C,K,A,L,F,E,H,
     * E	G,M,L,H,A,F,B,D,
     * F	L,M,D,C,G,A,
     * G	M,
     * H	O,
     * I	O,C,
     * J	O,
     * K	B,
     * L	D,E,
     * M	E,F,
     * O	A,H,I,J,F,
     * <p>
     * 结果最后不用单独处理逗号,中间数据格式在最后的MR中处理掉就行
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        sb.delete(0, sb.length());
        for (Text value : values) {
            sb.append(value.toString()).append(",");
        }
        v.set(sb.toString());
        context.write(key, v);
    }
}
