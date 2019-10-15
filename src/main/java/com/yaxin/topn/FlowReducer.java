package com.yaxin.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //这里已经是处理全部数据了,只有一个分组
        Iterator<Text> iterator = values.iterator();
        for (int i = 0; i < 10; i++) {
            //按照降序排序,取前10个即可
            if (iterator.hasNext()) {
                context.write(iterator.next(), key);
            }
        }
    }
}
