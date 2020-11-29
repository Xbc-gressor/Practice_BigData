package com.demo.salary;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalaryMapper extends Mapper<LongWritable, Text, Text, Text>{
	/*
	 * 重写 map 方法
	 * 将<k1,v1>转换成<k2,v2>
	 * <k1,v1>  0   大工 3-4年经验 6-8千/月 武汉 大数据开发工程师
	 * <k2,v2>  3-4年经验 6-8千/月
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// 将 v1 转换成 String 类型，并且 按照空格分隔为数组
		String[] valStr= value.toString().split("\\s+");
		// 数据清洗
		// 获取工作年限
		String work_year = "";
		for (int i = 0; i < valStr.length; i++) { // 包含经验
			if(Pattern.compile("\\u7ecf\\u9a8c").matcher(valStr[i]).find())
			{
				//System.out.println(valStr[i]);
				work_year = valStr[i];
				break;
			}
		}
		// 获取薪资
		String salary = "";
		for (int i = 0; i < valStr.length; i++) { // 包含/年、月
			if(Pattern.compile("/[\\u5e74\\u6708]").matcher(valStr[i]).find())
			{
				//System.out.println(valStr[i]);
				salary = valStr[i];
				break;
			}
		}


		// 流程控制
		if(!work_year.equals("") && !salary.equals("")){
			context.write(new Text(work_year), new Text(salary));
		}
//		if(!work_year.equals("") && valStr.length > 2 &&
//				Pattern.compile("[0-9]").matcher(valStr[2]).find() && Pattern.compile("/").matcher(valStr[2]).find()) {
//			String salary = valStr[2];
//			//System.out.println(salary);
//			// 转换成<k2,v2>
//			context.write(new Text(work_year), new Text(salary));
//		}

	}


}
