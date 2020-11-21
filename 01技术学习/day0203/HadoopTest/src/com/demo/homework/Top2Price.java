package com.demo.homework;

/*
    遇到输出为乱码：
        因为：Hadoop处理GBK文本时，发现输出出现了乱码，原来HADOOP在涉及编码时都是写死的UTF-8，如果文件编码格式是其它类型（如GBK)，则会出现乱码。
        解决方法：
            1. 将文件编码格式设置为UTF-8
 */
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.demo.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//mapreduce中top-n问题
public class Top2Price {


    public static class Top2PriceMapper extends Mapper<LongWritable, Text, Text, Text> {
        //每次读取一行
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] reads = value.toString().trim().split(",");
            String orderNumber = reads[0] +'\t' + reads[1];

            Double price = Double.parseDouble(reads[reads.length-2]) * Double.parseDouble(reads[reads.length-1]);
            String object = reads[2] + " " + price;
            System.out.println(reads[2]);
            context.write(new Text(orderNumber), new Text(object));
        }
    }


    public static class Top2PriceReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Double> map = new HashMap<String, Double>();
            for (Text val : values) {
                String[] s = val.toString().split(" ");
                map.put(s[0], Double.parseDouble(s[1]));
            }
            List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(map.entrySet());
            //排序
            Collections.sort(list, new Comparator<Entry<String, Double>>() {

                @Override
                public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
                    return (o2.getValue() - o1.getValue() > 0) ? 1 : -1;
                }
            });

            String top2 = list.get(0).getKey() + " " + list.get(0).getValue()
                    + "        " + list.get(1).getKey() + " " + list.get(1).getValue();
            context.write(new Text(key), new Text(top2));
        }




    }

    public static void main(String[] args)throws Exception {
        //hadoop运行环境和window有些问题，可以使用这行代码
        System.setProperty("hadoop.home.dir", "D:\\Fighting\\otherSubject\\AI\\计科实训_大数据\\资料\\hadoop-2.7.2");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");
        job.setInputFormatClass(TextInputFormat.class);
        //指定main方法所在的类
        job.setJarByClass(Top2Price.class);
        //指定本业务job要使用的mapper业务类
        job.setMapperClass(com.demo.homework.Top2Price.Top2PriceMapper.class);
        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //指定本业务job要使用的Reducer业务类
        job.setReducerClass(com.demo.homework.Top2Price.Top2PriceReducer.class);
        //指定最终输出的数据kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path("Data\\orderproduct.txt");
        Path outputPath = new Path("output");
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        //指定job的输入原始文件所在的目录以及输出结果
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        //提交作业等待执行完成，该方法的布尔参数为true表示把作业进度写到控制台
        boolean isdone = job.waitForCompletion(true);
        //执行成功(true)失败（false）
        System.out.print(isdone ? "执行成功" : "执行失败");
    }

}
