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

public class PrintCluster{
	public static boolean run(String globalHdfsPath, String meansPath, String srcDataPath, String printClusterPath) throws IOException, 				ClassNotFoundException, InterruptedException {

		/*
		globalHdfsPath = "hdfs://127.0.1.1:9000";
		meansPath = "/project2/result/k-means.txt";
		tmpMeansPath = "/project2/result/k-means_tmp";
		srcDataPath = "/project2/data/cluster.txt";
		printClusterPath = "/project2/result/cluster_result";
		*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		conf.set("KMeans", meansPath);
		Job job = Job.getInstance(conf, "print-cluster");
		job.setJarByClass(PrintCluster.class);

		// Map设置
		job.setMapperClass(printMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		// Reduce设置
		job.setReducerClass(printReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(srcDataPath));//srcDataPath = "/project2/data/cluster.txt";
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(printClusterPath));///project2/result/cluster_result

		return job.waitForCompletion(true);
	}

	static class printMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		
		//存k个中心值
		ArrayList<String> points = new ArrayList<>();
		int k;

		//启动map之前，读k-means.txt中k个中心值的数据
		protected void setup(Context context) throws IOException {
			String pointsData = FileOperator.getContentFromHDFS(context.getConfiguration().get("fs.defaultFS"),
					context.getConfiguration().get("KMeans"));
			String[] eachPointData = pointsData.split("\n");
			points.addAll(Arrays.asList(eachPointData));
			k = points.size();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int nearestPointNo = 0;
			double nearestDis = Double.MAX_VALUE;
			//计算每个点与k个中心点的距离，选择最近的中心点聚类
			for (int i = 0; i < k; i++) {
				Double tmpDis = Util.getDistance(value.toString(), points.get(i));
				if (tmpDis < nearestDis) {
					nearestDis = tmpDis;
					nearestPointNo = i;
				}
			}
			//k-v,输出行号-所属聚类点序号
			context.write(key, new LongWritable(nearestPointNo));
		}
	}

	static class printReducer extends Reducer<LongWritable, LongWritable, NullWritable, LongWritable> {
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, 		InterruptedException {
			for(LongWritable kind:values){
				//直接输出到文件cluster_result
				context.write(NullWritable.get(), kind);
			}
		}
	}
}
