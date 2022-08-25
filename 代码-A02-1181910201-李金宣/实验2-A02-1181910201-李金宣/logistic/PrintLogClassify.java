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

public class PrintLogClassify {
	public static boolean run(String globalHdfsPath, String thetaPath, String testDataPath, String printClassifyPath) throws 				IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		conf.setDouble("lr", 0.05);
		conf.set("theta", thetaPath);
		Job job = Job.getInstance(conf, "logistic");
		job.setJarByClass(LogisticRegression.class);

		// Map设置
		job.setMapperClass(printMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Reduce设置
		job.setReducerClass(printReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path(testDataPath));//val.txt测试文件input
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(printClassifyPath));//Logistic_result输出output

		return job.waitForCompletion(true);
	}
	static class printMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		public Double[] Xi = null;
		public Double[] theta = null;
		@Override
		//加载参数
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			Xi = new Double[20];
			theta = new Double[20];
			String[] thetaData = FileOperator.getContentFromHDFS(conf.get("fs.defaultFS"), conf.get("theta")).split("\n");
			for(int i=0;i<20;i++){
				theta[i] = Double.parseDouble(thetaData[i].split(" ")[1]);//获取20个参数
			}
		}
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] temp = value.toString().split(",");
			for (int i = 0; i < 20; i++) {
				Xi[i] = Double.parseDouble(temp[i]);//加载x
			}
			double exp = 0;
			for (int i = 0; i < 20; i++) {
				exp += (Xi[i] * theta[i]);//计算似然函数
			}
			double predict = 1 / (1 + (Math.exp(-exp)));//计算出似然概率
			Text re = new Text();
			if(predict > 0.5){//sigmoid函数大于0.5即为1类型，小于0.5即为0类型
				re.set("1");
			}else{
				re.set("0");
			}
			//k-v,行号-类型
			context.write(key, re);
		}
	}

	static class printReducer extends Reducer<LongWritable, Text, NullWritable, Text>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text value:values) {
				context.write(NullWritable.get(), value);//直接输出
			}
		}
	}
}
