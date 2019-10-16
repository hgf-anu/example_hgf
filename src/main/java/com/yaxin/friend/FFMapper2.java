package com.yaxin.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    /**
     * A	I,K,C,B,G,F,H,O,D,
     * 第一次切分为A和后面的部分
     * Mapper端处理数据为A,B都关注了C
     * A-B	 C
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        v.set(split[0]);
        String[] men = split[1].split(",");
        //双重循环
        for (int i = 0; i < men.length; i++) {
            for (int j = i + 1; j < men.length; j++) {
                //把小的字母放前面
                if (men[i].compareTo(men[j]) > 0) {
                    k.set(men[j] + "-" + men[i]);
                } else {
                    k.set(men[i] + "-" + men[j]);
                }
                context.write(k, v);
            }
        }

    }
}
