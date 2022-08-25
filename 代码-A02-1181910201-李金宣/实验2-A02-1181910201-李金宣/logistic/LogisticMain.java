
import java.io.IOException;
import java.util.Random;

public class LogisticMain {
	public static String globalHdfsPath = "hdfs://127.0.1.1:9000";
	public static String OutPath = "/project2/result/tmp_theta", thetaPath = "/project2/result/theta.txt";
	public static String srcDataPath = "/project2/data/train.txt", testDataPath = "/project2/data/val.txt";
	public static String printClassifyPath = "/project2/result/Logistic_result";
	public static int epoch = 10;

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		prepareThetaFile();
		//迭代epoch轮
		for (int i = 0; i < epoch; i++) {
			System.out.println("running " + i + "th epoch.");
			LogisticRegression.run(globalHdfsPath, thetaPath, srcDataPath, OutPath);
			FileOperator.rm(globalHdfsPath, thetaPath);
			//tmp_theta重命名为theta.txt
			if (FileOperator.rename(globalHdfsPath, OutPath + "/part-r-00000", thetaPath)) {
			FileOperator.rmdir(globalHdfsPath, OutPath);
			} 
		}
		PrintLogClassify.run(globalHdfsPath, thetaPath, testDataPath, printClassifyPath);
	}
	// 初始化文件
	public static void prepareThetaFile() throws IOException {
		FileOperator.rmdir(globalHdfsPath, printClassifyPath);
		FileOperator.rmdir(globalHdfsPath, OutPath);
	
		FileOperator.rm(globalHdfsPath, thetaPath);
		FileOperator.touch(globalHdfsPath, thetaPath);
		
		Random r = new Random(System.currentTimeMillis());
		StringBuilder dataToWrite = new StringBuilder();
		for (int i = 0; i < 20; i++) {
			dataToWrite.append(i + " ");
			dataToWrite.append(r.nextDouble());
			dataToWrite.append("\n");
		}
		dataToWrite.deleteCharAt(dataToWrite.length() - 1);
		FileOperator.appendContentToFile(globalHdfsPath, dataToWrite.toString(), thetaPath);
		
		
	}
}
