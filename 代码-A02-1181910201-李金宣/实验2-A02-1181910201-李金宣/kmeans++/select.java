import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class select {
	public static boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		/*
		globalHdfsPath = "hdfs://127.0.1.1:9000";
		meansPath = "/project2/result/k-means.txt";
		tmpMeansPath = "/project2/result/k-means_tmp";
		srcDataPath = "/project2/data/cluster.txt";
		printClusterPath = "/project2/result/cluster_result";
		*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://127.0.1.1:9000");
		Job job = Job.getInstance(conf, "d-Sort");
		job.setJarByClass(select.class);

		// Map设置
		job.setMapperClass(selectMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce设置
		job.setReducerClass(selectReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path("/project2/result/sort.txt"));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path("/project2/result/k-means.txt"));

		return job.waitForCompletion(true);
	}

	static class selectMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//按距离顺序均匀取20个点
			if(key%75000==0)
			context.write(new key, value); 
		}
	}

	static class selectReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value:values){
				//直接输出到文件
				context.write(NullWritable.get(), value);
			}
			context.write(NullWritable.get(), value);
		}
	}
}
