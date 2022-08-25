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
	
	}
}
