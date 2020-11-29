package com.demo.salary;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalaryReduce extends Reducer<Text, Text, Text, Text>{
	/**
	 *  将<k2,v2>转换为<k3,v3>
	 *  <k2,v2> 3-4年经验 <6-8千/月,5-9千/月,6-9千/月>
	 *  <k3,v3>  3-4年经验  5.6-9千/月  取最低薪资和最高薪资组成区间
	 *
	 *  [5.6,9千/月]
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// 最低薪资
		Double low = 0d;
		// 最高薪资
		Double hight = 0d;
		// 计数器
		int count = 0;
		// 因为k2 是一个集合 遍历集合
		for(Text value : values) {
			// 将 Text 转换成 String
			String text = value.toString();
			// 将薪资的字符串分隔为数组
			String[] strArr = text.split("-");

			//////// 取得最低和最高值 //////////////
			Double tLow, tHigh;
			// 如果没有范围，低值和高值相等
			if(strArr.length==1)
				tHigh = tLow = filterSalary(strArr[0]);
			else{
				// 获取每个字符串中的最低值
				tLow = filterSalary(strArr[0]);
				// 获取每个字符串中的最高值
				tHigh = filterSalary(strArr[1]);
			}

			// 转换成 k/month标准形式
			// 千
//			boolean c = Pattern.compile("\\u5343").matcher(text).find();

			boolean a = Pattern.compile("\\u4e07").matcher(text).find();
			if (a == true){ // 万
				//System.out.println(text);
				tLow *= 10;
				tHigh *= 10;
			}
			boolean b = Pattern.compile("\\u5e74").matcher(text).find();
			if (b == true){ // 年
				//System.out.println(text);
				tLow /= 12;
				tHigh /= 12;
			}
//			if (!a && !c) // 过滤掉 ../天 的数据
//			{
//				//System.out.println(text);
//				continue;
//			}
			// 按照我们的规则进行 v3的转换
			if(count == 0 || low > tLow) {
				low = tLow;
			}
			if(count == 0 || hight < tHigh) {
				hight = tHigh;
			}
			count++;
		}
		DecimalFormat df = new DecimalFormat("0.00");
		// 输出 k3 v3
		context.write(key, new Text(df.format(low)+"-"+df.format(hight)+"k/month"));

	}
	/**
	 * 薪资中的中文过滤
	 * @param salary
	 * @return
	 */
	public Double filterSalary(String salary) {
		// 匹配 非数字 小数点的内容，替换为""
		String sal = Pattern.compile("[^\\d.]+").matcher(salary).replaceAll("");
		return Double.parseDouble(sal);
	}

}
