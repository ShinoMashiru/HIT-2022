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

public class NaiveBayes {
	public static boolean run(String globalHdfsPath, String trainDataPath, String formalParamPath) throws IOException, 					ClassNotFoundException, InterruptedException {
		
		/*
		globalHdfsPath = "hdfs://127.0.1.1:9000";
	 	OutPath = "/project2/result/Bayes", paramPath = "/project2/result/avgD.txt";
	 	srcDataPath = "/project2/data/train.txt", testDataPath = "/project2/data/val.txt";
	 	printClassifyPath = "/project2/result/Classify_result";
		*/
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		Job job = Job.getInstance(conf, "Bayes");
		job.setJarByClass(NaiveBayes.class);

		// Map设置
		job.setMapperClass(BayesMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce设置
		job.setReducerClass(BayesReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		//输出文件设置
		FileInputFormat.setInputPaths(job, new Path(trainDataPath));//train.txt训练数据集input
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(formalParamPath));//Bayes文件output

		return job.waitForCompletion(true);
	}

	static class BayesMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String point = value.toString();
			Text classType = new Text(point.substring(point.length() - 1));// 获得类型
			Text data = new Text(point.substring(0, point.length() - 2));// 获得数据
			//k-v,类型（0，1）-数据，传递到reducer
			context.write(classType, data);
		}
	}

	static class BayesReducer extends Reducer<Text, Text, NullWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			//存储均值和方差
			double[] mean = new double[20];
			double[] D = new double[20];


			ArrayList<ArrayList<Double>> points = new ArrayList<>();
			for (Text value : values) {
				count++;
				String[] tmpPointS = value.toString().split(",");
				ArrayList<Double> tmpPointD = new ArrayList<>();
				//计算20维数据每一纬的均值和方差
				for (int i = 0; i < 20; i++) {
					double tmpCoordinate = Double.parseDouble(tmpPointS[i]);
					tmpPointD.add(tmpCoordinate);
					mean[i] += tmpCoordinate;
				}
				points.add(tmpPointD);
			}
			// 均值
			for (int i = 0; i < 20; i++) {
				mean[i] /= count;
			}
			// 方差
			for (ArrayList<Double> point : points) {
				for (int i = 0; i < 20; i++) {
					D[i] += (point.get(i) - mean[i]) * (point.get(i) - mean[i]);
				}
			}
			for (int i = 0; i < 20; i++) {
				D[i] /= count;
			}
			//输出40个参数（均值、方差）
			StringBuilder re = new StringBuilder();
			for (int i = 0; i < 20; i++) {
				re.append(mean[i]).append(",");
			}
			for (int i = 0; i < 20; i++) {
				re.append(D[i]).append(",");
			}
			
			//均值，方差，类型的概率，类型，写入Bayes
			re.append((double) count/ 900000.0 + ",");
			re.append(key.toString());
			context.write(NullWritable.get(), new Text(re.toString()));
		}
	}

}
