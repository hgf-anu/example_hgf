package com.yaxin.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    private Text k = new Text();
    private Text v = new Text();

    /**
     * 转换视角,我关注了谁->谁关注了我
     * A:B,C,D,F,E,O指的是A关注了B,C,D,F,E,O,进行一个行转列,Mapper端转换为:
     * B:A
     * C:A
     * D:A
     * ..
     * Reducer端转换为 [我:我关注的人]
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        //关注别人的人作为Value
        v.set(split[0]);

//        System.out.println("数组1长度:"+split.length);
        //被关注的人作为Key
        String[] split1 = split[1].split(",");
        for (String man : split1) {
//            System.out.println("数组2的长度:"+split1.length);
            k.set(man);
            context.write(k, v);
        }
    }


}
