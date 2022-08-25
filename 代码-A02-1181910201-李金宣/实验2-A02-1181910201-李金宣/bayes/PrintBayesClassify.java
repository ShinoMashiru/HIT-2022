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

public class PrintBayesClassify {
	public static boolean run(String globalHdfsPath, String avdDPath, String srcDataPath, String printClassifyPath) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		conf.set("avg_D", avdDPath);
		Job job = Job.getInstance(conf, "print-classify");
		job.setJarByClass(PrintBayesClassify.class);

		// Map设置
		job.setMapperClass(printMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		// Reduce设置
		job.setReducerClass(printReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path(srcDataPath));//val.txt测试文件input
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(printClassifyPath));//Classify_result分类结果output

		return job.waitForCompletion(true);
	}

	static class printMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		ArrayList<Double> positiveMeans, positiveD, negativeMeans, negativeD;
		Double positivePriori, negativePriori;

		//加载均值、方差、先验
		protected void setup(Context context) throws IOException {
			String paramData = FileOperator.getContentFromHDFS(context.getConfiguration().get("fs.defaultFS"),
					context.getConfiguration().get("avg_D"));

			positiveMeans = new ArrayList<>();	//1类型平均值
			positiveD = new ArrayList<>();		//1类型方差
			negativeMeans = new ArrayList<>();	//0类型平均值
			negativeD = new ArrayList<>();		//0类型方差
			String[] param = paramData.split("\n");
			for (int i = 0; i < 2; i++) {
				String[] tmp = param[i].split(",");
				if (tmp[tmp.length - 1].equals("0")) {
					for (int j = 0; j < 20; j++) {
						negativeMeans.add(Double.parseDouble(tmp[j]));//0类型均值
					}
					for (int j = 20; j < 40; j++) {
						negativeD.add(Double.parseDouble(tmp[j]));//0类型方差
					}
					negativePriori = Math.log(Double.parseDouble(tmp[tmp.length - 2]));//0类型概率
				} else {
					for (int j = 0; j < 20; j++) {
						positiveMeans.add(Double.parseDouble(tmp[j]));//1类型均值
					}
					for (int j = 20; j < 40; j++) {
						positiveD.add(Double.parseDouble(tmp[j]));//1类型方差
					}
					positivePriori = Math.log(Double.parseDouble(tmp[tmp.length - 2]));//1类型概率
				}
			}
		}
		//使用连续型贝叶斯的正太概率分布函数，计算属于类型的概率（将函数取了log方便计算，因为单调性不变，不影响大小比较）
		private double possible(double x, int c, int i) {
			double tmpMean, tmpD;
			if (c==0) {
				tmpD = negativeD.get(i);
				tmpMean = negativeMeans.get(i);
			} else {
				tmpD = positiveD.get(i);
				tmpMean = positiveMeans.get(i);
			}
			return -(Math.log(2) + Math.log(Math.PI)) / 2.0 - Math.log(tmpD) - (Math.pow((x - tmpMean) / tmpD, 2.0) / 2.0);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int kind;
			double pPositive = positivePriori, pNegative = negativePriori;
			String[] pointData = value.toString().split(",");
			double[] point = new double[20];
			for(int i=0;i<20;i++){
				point[i] = Double.parseDouble(pointData[i]);
			}
			for(int i=0;i<20;i++){
				pPositive += possible(point[i], 1, i);//计算1类型的后验概率
				pNegative += possible(point[i], 0 ,i);//计算0类型的后验概率
			}
			if(pPositive > pNegative){		//根据后验概率大小，决定类型
				kind = 1;
			}else{
				kind = 0;
			}
			//k-v,行号-类别送到reducer
			context.write(key, new LongWritable(kind));
		}
	}

	static class printReducer extends Reducer<LongWritable, LongWritable, NullWritable, LongWritable> {
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			for (LongWritable kind : values) {
				context.write(NullWritable.get(), kind);//直接输出
			}
		}
	}
}
