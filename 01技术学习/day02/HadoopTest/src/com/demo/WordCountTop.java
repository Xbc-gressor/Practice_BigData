package com.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
public class WordCountTop {
	/**
	 * KEYIN 默认情况下，是mr框架读到一行数据的起始偏移量
	 * VALUEIN 默认情况下，是mr框架读到一行文本内容的值
	 *
	 * KEYOUT 用户自定义逻辑处理完成之后的key 单词 String
	 * VALUEOUT 用户自定义逻辑处理完成之后的value 单词 Integer
	 *
	 **/

	public static class WordCountTopMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		//每次读取一行
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			//按照空格切分单词
			String[] words = line.split(" ");
			//将单词输出为<单词，1>
			for(String word : words) {
				//将单词作为key,次数1作为value,以便于后续数据分发reduce task
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}
	/**
	 *  KEYIN VALUEIN 对应mapper输出的KEYOUT VALUEOUT类型对应
	 *  KEYOUT VALUEOUT 是自定义reduce逻辑处理结果的输出数据
	 *
	 */

	public static class WordCountTopReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		//定义集合map存储每组单词和单词出现次数
		Map<String,Integer> map=new HashMap<String, Integer>();
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//循环求次数
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();

			}
			//单词
			String word=key.toString();
			//把单词和单词出现次数存放在map中，方便后续的排序
			map.put(word, count);


		}
		//全局处理reduce中所有数据map对象
		protected void cleanup(Context context)throws IOException, InterruptedException {
			//map.entrySet()把map的键值对转化为Set集合
			//然后把set集合转化为list集合方便排序
			//Entry:键值对对象
			List<Entry<String,Integer>> list = new ArrayList<Entry<String,Integer>>(map.entrySet());
			//排序
			Collections.sort(list,new Comparator<Entry<String,Integer>>(){

			@Override
			public int compare(Entry<String, Integer> e1,Entry<String, Integer> e2) {
				 //Entry对象的值进行降序排序，e2-e1降序   e1-e2升序
				return (int) (e2.getValue() - e1.getValue());
			}});

			//排序后再取得集合中数据的前2名的数据
			 for(int i=0;i<2;i++){
				 //单词:list.get(i).getKey(),list.get(i).getValue()单词出现次数
		           context.write(new Text(list.get(i).getKey()), new IntWritable(list.get(i).getValue()));

		    }



		}





	}

	public static void main(String[] args)throws Exception {
		//hadoop运行环境和window有些问题，可以使用这行代码
		System.setProperty("hadoop.home.dir", "D:\\Fighting\\otherSubject\\AI\\计科实训_大数据\\资料\\hadoop-2.7.2");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "WordCount");
		job.setInputFormatClass(TextInputFormat.class);
		//指定main方法所在的类
		job.setJarByClass(WordCount.class);
		//指定本业务job要使用的mapper业务类
		job.setMapperClass(WordCountTopMapper.class);
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//指定本业务job要使用的Reducer业务类
		job.setReducerClass(WordCountTopReducer.class);
		//指定最终输出的数据kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path("Data\\wordcount.txt");
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
