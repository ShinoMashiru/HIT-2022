import java.io.IOException;

public class BayesMain {
	public static String globalHdfsPath = "hdfs://127.0.1.1:9000";
	public static String OutPath = "/project2/result/Bayes", paramPath = "/project2/result/avgD.txt";
	public static String srcDataPath = "/project2/data/train.txt", testDataPath = "/project2/data/val.txt";
	public static String printClassifyPath = "/project2/result/Classify_result";
	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		
		FileOperator.rm(globalHdfsPath, paramPath);
		FileOperator.rmdir(globalHdfsPath, printClassifyPath);
		
		if(NaiveBayes.run(globalHdfsPath, srcDataPath, OutPath)) {
			System.out.println("avg and D have been written into " + OutPath);
		}
		
		//Bayes重命名为avgD.txt，删除Bayes
		FileOperator.rename(globalHdfsPath, OutPath + "/part-r-00000", paramPath);
		FileOperator.rmdir(globalHdfsPath, OutPath);
		
		if(PrintBayesClassify.run(globalHdfsPath, paramPath, testDataPath, printClassifyPath)){
			System.out.println("result has been moved into " + printClassifyPath);
		}
	}
}
