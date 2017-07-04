package liang_task;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class query_rank_8 {
	public static int sum1 = 0;
	public static int sum2 = 0;

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			sum2++;
			String[] str = value.toString().split("\t");
			int rank = Integer.parseInt(str[3]);
			if(rank<11)
			{
				sum1=sum1+1;
				
			}	
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.23.9:9000");
		Job job = Job.getInstance(conf, "query_rank_8");
		job.setMapperClass(MyMapper.class);
		
		job.setOutputKeyClass(Text.class);
		
		job.setOutputValueClass(Text.class);
		
		job.setJarByClass(query_rank_8.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.out.println("sum1="+sum1+"\tsum2="+sum2);
		float percentage = (float)sum1/(float)sum2;
		System.out.println("Rank在10以内的点击次数占比：" +percentage);
		System.out.println("count over");
	}

}
