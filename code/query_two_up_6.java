package liang_task;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class query_two_up_6 {
	/*
	 * 
	 * InputFormat ->FileInputFormat(子类)-> TextInputFormat(子类)
	 * 程序当中默认调用的就是TextInputFormat 1.验证数据路径是否合法 2.TextInputFormat默认读取数据，就是一行一行读取的
	 * 
	 * 开始执行map，一个map就是一个task（是一个Java进程，运行在JVM上的）
	 * map执行完毕后，会执行combiner（可选项），对一个map中的重复单词进行合并，value是一个map中出现的相同单词次数。
	 * shuffle(partitioner分区，进行不同map的值并按key排序)
	 * Reducer接收所有map数据，并将具有相同key的value值合并
	 * 
	 * OutputFormat ->FileOutputFormat(子类)->TextOutputFormat(子类) 1.验证数据路径是否合法
	 * 2.TextOutputFormat写入文件格式是 key + "\t" + value + "\n"
	 */

	// Text是一个能够写入文件的Java String数据类型
	// IntWritable是能够写入文件的int数据类型
	// 头两个参数表示的是输入数据key和value的数据类型
	// 后两个数据表示的就是输出数据key和value的数据类型
	public static int total = 0;
	public static class MyMaper extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		// key是行地址
		// value是一行字符串
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String[] str = value.toString().split("\t");
			Text word;
			IntWritable one = new IntWritable(1);
			word = new Text(str[1]);
			context.write(word, one);
			// 执行完毕后就是一个单词 对应一个value(1)
		}

	}

	// 头两个参数表示的是输入数据key和value的数据类型
	// 后两个数据表示的就是输出数据key和value的数据类型
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context arg2) throws IOException, InterruptedException {

			// arg0是一个单词 arg1是对应的次数
			int sum = 0;
			for (IntWritable i : arg1) {
				sum += i.get();
			}
			if(sum>2){
				total=total+1;
			}
			//arg2.write(arg0, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("query_two_up_6 begin");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.23.9:9000");
		// 1.实例化一个Job
		Job job = Job.getInstance(conf, "query_two_up_6");
		// 2.设置mapper类
		job.setMapperClass(MyMaper.class);
		// 3.设置Combiner类 不是必须的
		// job.setCombinerClass(MyReducer.class);
		// 4.设置Reducer类
		job.setReducerClass(MyReducer.class);
		// 5.设置输出key的数据类型
		job.setOutputKeyClass(Text.class);
		// 6.设置输出value的数据类型
		job.setOutputValueClass(IntWritable.class);
		// 设置通过哪个类查找job的Jar包
		job.setJarByClass(query_two_up_6.class);
		// 7.设置输入路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 8.设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 9.执行该作业
		job.waitForCompletion(true);
		System.out.println("查询次数大于2次的用户总数：" + total + "条");
		System.out.println("over");

	}

}
