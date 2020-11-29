package com.demo.salary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;

// 继承配置，实现任务的启动
public class Jobmain extends Configured implements Tool {


	@Override
	public int run(String[] arg0) throws Exception {

		// 1. 创建 job 对象
		Job job = Job.getInstance(super.getConf(), "salary");
		// 设置 jar 主类
		job.setJarByClass(Jobmain.class);

		//2. 配置 job 任务 八大步骤
		// 设置输入类型
		job.setInputFormatClass(TextInputFormat.class);
		// 设置读取文件的路径
		TextInputFormat.addInputPath(job, new Path("Data\\51job_salary.txt"));
		// 设置自定义的 Mapper 类
		job.setMapperClass(SalaryMapper.class);
		// 设置Map阶段的输出数据类型
		// key
		job.setMapOutputKeyClass(Text.class);
		// value
		job.setMapOutputValueClass(Text.class);

		// 第 三、四、五、六的 shuffle 阶段采用默认

		///// 设置定义的 Reducer 类
		job.setReducerClass(SalaryReduce.class);
		///// 设置 reduce 阶段 输出的 k3,v3的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		///// 设置 输出的类
		job.setOutputFormatClass(TextOutputFormat.class);
		///// 输出的路径
			// 已经存在，要删除原目录
		File dirFile = new File("output\\salaryOut");
		if (dirFile.exists()){
			File files[] = dirFile.listFiles();
			for(int i=0;i<files.length;i++)
				files[i].delete();
			dirFile.delete();
			System.out.println("覆盖已有的结果！");
		}
		Path outputPath = new Path("output\\salaryOut");
		TextOutputFormat.setOutputPath(job, outputPath);
//		if(fs.exists(outputPath)) {
//			fs.delete(outputPath, true);
//		}

		// 3.等待任务结束
		boolean b = job.waitForCompletion(true);
		// 返回值为 int 类型 , 将 boolean 转 int
		return b?0:-1;
	}

	public static void main(String[] args) throws Exception {
		// 添加这行代码，避windows坑
		Configuration configuration = new Configuration();
		System.setProperty("hadoop.home.dir", "D:\\Fighting\\otherSubject\\AI\\计科实训_大数据\\资料\\hadoop-2.7.2");
		int run = ToolRunner.run(configuration, new Jobmain(), args);
		System.exit(run);
	}

}
