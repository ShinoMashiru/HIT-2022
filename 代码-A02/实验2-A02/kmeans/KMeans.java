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

public class KMeans {
	public static boolean run(String globalHdfsPath, String meansPath, String srcDataPath, String tmpMeansPath) throws IOException, 		ClassNotFoundException, InterruptedException {
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
		Job job = Job.getInstance(conf, "K-means");
		job.setJarByClass(KMeans.class);

		// Map设置
		job.setMapperClass(KMeansMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce设置
		job.setReducerClass(KMeansReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(srcDataPath));//srcDataPath = "/project2/data/cluster.txt
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(tmpMeansPath));
		//tmpMeansPath = "/project2/result/k-means_tmp

		return job.waitForCompletion(true);
	}

	static class KMeansMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
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
			for (int i = 0; i < k; i++) {
				//计算每个点与k个中心点的距离，选择最近的中心点聚类
				Double tmpDis = Util.getDistance(value.toString(), points.get(i));
				if (tmpDis < nearestDis) { 
					nearestDis = tmpDis;
					nearestPointNo = i;
				}
			}
			// k-v，k个中心点-中心点聚集的点，每个中心点的聚集的点送到同一个reducer处理
			context.write(new LongWritable(nearestPointNo), value); 
		}
	}

	static class KMeansReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//存储新的中心值
			double[] center = new double[20];
			for (int i = 0; i < 20; i++) {
				center[i] = 0.0;
			}
			double count = 0;

			//计算每个聚类新的中心值
			for (Text value : values) {
				String[] point = value.toString().split(",");
				for (int i = 0; i < 20; i++) {
					center[i] += Double.parseDouble(point[i]);
				}
				count++;
			}

			for (int i = 0; i < 20; i++) {
				center[i] /= count;	//计算平均值获得新的k个中心值
			}

			StringBuilder re = new StringBuilder();
			for (Double eachData : center) {
				re.append(eachData.toString());
				re.append(",");
			}

			re.deleteCharAt(re.length() - 1);
			//将新的k个中心值写入文件k-means_tmp
			context.write(NullWritable.get(), new Text(re.toString()));
		}
	}
}
