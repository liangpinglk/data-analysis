package liang_task;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class query_url_9 {
	public static int sum1 = 0;
	public static int sum2 = 0;

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");
			Pattern p = Pattern.compile("(\\w?)+\\.(com|cn|net|org|biz|info|cc|tv|top)");
			Matcher matcher = p.matcher(str[2]);
			matcher.find();
			try {
				if(matcher.group()!=null)
					sum1++;
					sum2++;
			} catch (Exception e) {
					sum2++;
			}

		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("query_url_9 begin");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.23.9:9000");
		Job job = Job.getInstance(conf, "query_url_9");
		job.setMapperClass(MyMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(query_url_9.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("sum1="+sum1+"\tsum2="+sum2);
		float percentage = (float)sum1/(float)sum2;
		System.out.println("直接用url查询的用户占比：" +percentage);
		System.out.println("count over");
	}

}
