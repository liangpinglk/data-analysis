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

public class query_three_uid_10 {
	public static String queryContent="仙剑奇侠传";
	public static class MyMaper extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		// key是行地址
		// value是一行字符串
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {

			String[] str = value.toString().split("\t");
			Text word;
			IntWritable one = new IntWritable(1);
			if(str[2].equals(queryContent)){
				word = new Text(str[1]);
				context.write(word, one);
			}
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
			if(sum>3){
			
				System.out.println(arg0 + ":" + sum);
			}
			arg2.write(arg0, new IntWritable(sum));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("query begin");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.23.9:9000");
		// 1.实例化一个Job
		Job job = Job.getInstance(conf, "query_three_uid_10");
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
		job.setJarByClass(query_three_uid_10.class);
		// 7.设置输入路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 8.设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 9.执行该作业
		job.waitForCompletion(true);
		System.out.println("over");

	}

}
