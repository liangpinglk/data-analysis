package liang_task;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class query_not_null2 {
	public static int sum = 0;

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] str = value.toString().split("\t");
			
			if (str[2]!=null)
				sum++;

		}

	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("query_not_null2 begin");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.23.9:9000");
		Job job = Job.getInstance(conf, "query_not_null");
		job.setMapperClass(MyMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(query_not_null2.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("非空数据共有：" + sum + "条");
		System.out.println("count over");
	}

}
