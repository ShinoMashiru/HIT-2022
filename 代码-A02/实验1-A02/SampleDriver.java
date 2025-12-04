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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class SampleDriver{


	public static void run()throws IOException, InterruptedException, ClassNotFoundException{
			
		
		Configuration conf_sample = new Configuration();
		conf_sample.set("fs.defaultFS", "hdfs://127.0.1.1:9000");
		Job job_sample = Job.getInstance(conf_sample,"Sample_Driver");
		job_sample.setJarByClass(SampleDriver.class);

		// Map设置
		job_sample.setMapperClass(SampleMapper.class);
		job_sample.setMapOutputKeyClass(Text.class);
		job_sample.setMapOutputValueClass(Text.class);

		// Reduce设置
		job_sample.setReducerClass(SampleReducer.class);
		job_sample.setOutputKeyClass(NullWritable.class);
		job_sample.setOutputValueClass(Text.class);

		// 输出文件设置
		LazyOutputFormat.setOutputFormatClass(job_sample, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job_sample, new Path("/data.txt"));
		FileOutputFormat.setOutputPath((JobConf) job_sample.getConfiguration(), new Path("/D_Sample"));
		
		job_sample.waitForCompletion(true);


	}
}
