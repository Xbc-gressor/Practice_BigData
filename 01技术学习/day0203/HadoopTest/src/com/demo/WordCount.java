package com.demo;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {						      // KEYIN, VALUEIN, KEYOUT, VALUEOUT
												      //    偏移量    输入    输出
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		/*
		 测试数据：
		 hello world
		 hello China
		 hello China
		 */

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			// 每次读取一行
			String line = value.toString();
			// 按照空格切分单词
			String[] words = line.split(" ");
			// 将单词输出为<单词, 1>
			for(String word: words) {
				// 将单词作为key， 次数1作为value， 以便于后续数据分发reduce task
				context.write(new Text(word), new IntWritable(1));
			}
			// 最后map的输出格式，按照key升序排序，(China, 1) (China, 1) (hello, 1) (hello, 1) (hello, 1) (world, 1)
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			// 一个单词出现的次数的和
			int sum = 0;
			for (IntWritable val: values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 添加这行代码，避windows坑
		System.setProperty("hadoop.home.dir", "D:\\Fighting\\otherSubject\\AI\\计科实训_大数据\\资料\\hadoop-2.7.2");
		Configuration conf = new Configuration();
		// 创建业务jib类
		Job job = Job.getInstance(conf, "WordCount");
		// 指定main方法所在的类
		job.setJarByClass(WordCount.class);

		// 指定本业务job索要使用的mapper业务类
		job.setMapperClass(WordCountMapper.class);
		// 指定，mapper输出的数据的key-value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 指定本业务要使用的Reducer业务类
		job.setReducerClass(WordCountReducer.class);
		// 指定最终的输出的数据kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileSystem fs = FileSystem.get(conf);
		// 输入测试文件的路径
		Path inputPath = new Path("Data:\\wordcount.txt");
		// 输出结果的文件路径
		Path outputPath = new Path("output");
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		// 指定job的输入原始文件所在的目录以及输出结果
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		// 提交作业等待执行完成，该方法的布尔参数为true表示把作业进度写到控制台
		boolean isDone = job.waitForCompletion(true);
		// 执行成功或失败，这个布尔值被转换成程序退出的代码0或1
		if(isDone) {
			System.out.println("执行成功");
		}else {
			System.out.println("执行失败");
		}
	}

}
