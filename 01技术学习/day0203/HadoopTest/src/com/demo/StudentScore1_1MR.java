package com.demo;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *计算同一门课程下分数相同的出现次数大于2次的次数和考生姓名
 */

public class StudentScore1_1MR {

	public static class StudentScore1MR_Mapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
//			computer,huangxiaoming,85
//			computer,xuzheng,54
//			computer,huangbo,86
			String [] reads = value.toString().trim().split(",");
			String coursescore = reads[0] + "\t" + reads[2];
			String name = reads[1];

			context.write(new Text(coursescore), new Text(name));
		}
	}



	public static class StudentScore1MR_Reducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException {
			//math,huangxiaoming,85
			int count = 0;
			StringBuilder sb = new StringBuilder();
			for(Text text : value){
				//累加学生人数
				count++;
				//拼接学生姓名
				sb.append(text.toString()).append(",");
			}
			if (count >= 2) {
				//去掉最后的一个逗号
				String result =count + "\t"+sb.toString().substring(0, sb.length() - 1);
				//reduce输出
				context.write(key,  new Text(result));
			}
		}
	}


	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\Fighting\\otherSubject\\AI\\计科实训_大数据\\资料\\hadoop-2.7.2");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(StudentScore1_1MR.class);
		job.setMapperClass(StudentScore1MR_Mapper.class);
		job.setReducerClass(StudentScore1MR_Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//

		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path("Data\\score1.txt");
		Path outputPath = new Path("output");
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}


		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isdone = job.waitForCompletion(true);
		System.out.print(isdone ? "执行成功" : "执行失败");
	}

}
