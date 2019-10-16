package com.yaxin.invertindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IIDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job1 = Job.getInstance(new Configuration());

        job1.setJarByClass(IIDriver.class);

        job1.setMapperClass(IIMapper1.class);
        job1.setReducerClass(IIReducer1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job1, new Path("d:\\input"));
        FileOutputFormat.setOutputPath(job1, new Path("d:\\output"));

        boolean b = job1.waitForCompletion(true);
        //这里不选择退出程序,如果返回true则执行第二个job
        //正常是分开两个类来写两个job的,这里是方便简单执行
        if (b) {
            Job job2 = Job.getInstance(new Configuration());

            job2.setMapperClass(IIMapper2.class);
            job2.setReducerClass(IIReducer2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job2, new Path("d:\\output"));
            FileOutputFormat.setOutputPath(job2, new Path("d:\\output2"));

            boolean b2 = job2.waitForCompletion(true);
            System.exit(b2 ? 0 : 1);
        }


    }
}
