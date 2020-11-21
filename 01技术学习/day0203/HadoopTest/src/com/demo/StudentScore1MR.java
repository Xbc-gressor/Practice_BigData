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
 *求得每门课程最高成绩，最低成绩，平均成绩
 */
public class StudentScore1MR {



	public static class StudentScore1MR_Mapper extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			//math,huangxiaoming,85
			String [] reads = value.toString().trim().split(",");
			String course = reads[0];
			String score = reads[2];

			context.write(new Text(course), new Text(score));
		}
	}
	public static class StudentScore1MR_Reducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException {
			//math,huangxiaoming,85
			int maxscore = Integer.MIN_VALUE;
			int minscore = Integer.MAX_VALUE;
			int avgscore = 0;
			int count = 0;
			int sum = 0;
			for(Text text : value){
				int score = Integer.parseInt(text.toString());
				if (maxscore < score) {
					maxscore = score;
				}
				if (minscore > score) {
					minscore = score;
				}
				sum += score;
				count++;
			}
			avgscore = sum / count;
			String result = "max="+maxscore+"\t" + "min=" + minscore + "\t" + "avg=" + avgscore;

			context.write(key, new Text(result));
		}
	}
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "D:\\Fighting\\otherSubject\\AI\\计科实训_大数据\\资料\\hadoop-2.7.2");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(StudentScore1MR.class);
		job.setMapperClass(StudentScore1MR_Mapper.class);
		job.setReducerClass(StudentScore1MR_Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path("Data\\score1.txt");
		Path outputPath = new Path("output");
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}


		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean isDone = job.waitForCompletion(true);
		System.out.print(isDone ? "执行成功" : "执行失败");
	}
}
