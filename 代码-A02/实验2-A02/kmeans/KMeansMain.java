import java.io.IOException;
import java.util.Random;

public class KMeansMain {
	public static String globalHdfsPath = "hdfs://127.0.1.1:9000";
	public static String meansPath = "/project2/result/k-means.txt";
	public static String tmpMeansPath = "/project2/result/k-means_tmp";
	public static String srcDataPath = "/project2/data/cluster.txt";
	public static String printClusterPath = "/project2/result/cluster_result";
	public static int k = 20, epoch = 20;

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		prepareMeansFile();
		
		//迭代epoch轮
		for (int i = 0; i < epoch; i++) {
			System.out.println("running " + i + "th epoch.");
			KMeans.run(globalHdfsPath, meansPath, srcDataPath, tmpMeansPath);
			//删除写有旧中心点的k-means.txt
			FileOperator.rm(globalHdfsPath, meansPath);
			//将写了新中心点的k-means_tmp重命名为k-means.txt
			if (FileOperator.rename(globalHdfsPath, tmpMeansPath + "/part-r-00000", meansPath)) {
				FileOperator.rmdir(globalHdfsPath, tmpMeansPath);
					} 
		}	
		//最后聚类结果输出到cluster_result
		PrintCluster.run(globalHdfsPath, meansPath, srcDataPath, printClusterPath);
	}
	// 随机选择中心点、初始化k-means.txt文件。
	public static void prepareMeansFile() {
		try {
			//删除k-means_tmp，cluster_result
			FileOperator.rmdir(globalHdfsPath, printClusterPath);
			FileOperator.rmdir(globalHdfsPath, tmpMeansPath);
			//创建k-means.txt
			FileOperator.touch(globalHdfsPath, meansPath);
		
			//随机生成k个中心点
			Random r = new Random(System.currentTimeMillis());
			StringBuilder dataToWrite = new StringBuilder();
			for (int i = 0; i < k; i++) {
				for (int j = 0; j < 20; j++) {
					dataToWrite.append(r.nextDouble() * 10.0 - 5.0);
					if (j < 19) {
						dataToWrite.append(",");
					}
				}
				dataToWrite.append("\n");
			}
			//写入k-means.txt
			FileOperator.appendContentToFile(globalHdfsPath, dataToWrite.toString(), meansPath);
		}catch(Exception e){};
	}
}
