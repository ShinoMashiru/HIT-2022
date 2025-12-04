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

public class DoneDriver{


	public static void run()throws IOException, InterruptedException, ClassNotFoundException{
			
		
		Configuration conf_done = new Configuration();
		conf_done.set("fs.defaultFS", "hdfs://127.0.1.1:9000");
		
		Job job_done = Job.getInstance(conf_done,"DoneDriver");
		job_done.setJarByClass(SampleDriver.class);
		// Map设置
		job_done.setMapperClass(DoneMapper.class);
		job_done.setMapOutputKeyClass(Text.class);
		job_done.setMapOutputValueClass(Traveler.class);

		// Reduce设置
		job_done.setReducerClass(DoneReducer.class);
		job_done.setOutputKeyClass(NullWritable.class);
		job_done.setOutputValueClass(Text.class);

		// 输出文件设置
		LazyOutputFormat.setOutputFormatClass(job_done, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job_done, new Path("/D_Sample/part-r-00000"));
		FileOutputFormat.setOutputPath((JobConf) job_done.getConfiguration(), new Path("/D_Filter"));

		job_done.waitForCompletion(true);

	}
}
