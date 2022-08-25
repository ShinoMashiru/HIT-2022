import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * 使用JAVA API和hdfs进行文件交互
 */
public class FileOperator {
	/**
	 * 检测hdfs是否存在文件
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param path           查询的文件路径
	 * @return 如果存在true, 否则false
	 * @throws IOException
	 */
	public static boolean testExit(String globalHdfsPath, String path) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		return fs.exists(new Path(path));
	}

	/**
	 * 创建目录
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param hdfsDir        目录路径
	 * @return 创建成功返回true, 否则返回false
	 * @throws IOException
	 */
	public static boolean mkdir(String globalHdfsPath, String hdfsDir) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(hdfsDir);
		boolean result = fs.mkdirs(hdfsPath);
		fs.close();
		return result;
	}

	/**
	 * 删除目录
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param hdfsDir        目录路径
	 * @return 删除成功返回true, 否则返回false
	 * @throws IOException
	 */
	public static boolean rmdir(String globalHdfsPath, String hdfsDir) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(hdfsDir);
		boolean result = fs.delete(hdfsPath, true);
		fs.close();
		return result;
	}

	/**
	 * 创建文件
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param hdfsFilePath   文件名
	 * @throws IOException
	 */
	public static void touch(String globalHdfsPath, String hdfsFilePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path filePath = new Path(hdfsFilePath);
		FSDataOutputStream outputStream = fs.create(filePath);
		outputStream.close();
		fs.close();
	}
	

	public static boolean rename(String globalHdfsPath, String hdfsSrcFile, String hdfsTargetFile) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(hdfsSrcFile);
		Path tarPath = new Path(hdfsTargetFile);
		boolean result = fs.rename(srcPath, tarPath);
		fs.close();
		return result;
	}

	/**
	 * 删除文件
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param hdfsFilePath   文件名
	 * @return
	 * @throws IOException
	 */
	public static boolean rm(String globalHdfsPath, String hdfsFilePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path filePath = new Path(hdfsFilePath);
		boolean result = fs.delete(filePath, false);
		fs.close();
		return result;
	}

	/**
	 * 追加内容到文件结尾
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param content        追加的内容
	 * @param hdfsFilePath   文件路径
	 * @throws IOException
	 */
	public static void appendContentToFile(String globalHdfsPath, String content, String hdfsFilePath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path filePath = new Path(hdfsFilePath);
		FSDataOutputStream out = fs.append(filePath);
		out.write(content.getBytes());
		out.close();
		fs.close();
	}

	/**
	 * 复制本地文件到hdfs
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param localPath      本地文件路径
	 * @param hdfsPath       hdfs文件路径
	 * @throws IOException
	 */
	public static void putFileToHDFS(String globalHdfsPath, String localPath, String hdfsPath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path localFile = new Path(localPath);
		Path hdfsFile = new Path(hdfsPath);
		fs.copyFromLocalFile(false, true, localFile, hdfsFile);
	}

	/**
	 * 从hdfs复制文件到本地, 若名字重复追加_0, 01
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param localPath      本地文件路径
	 * @param hdfsPath       hdfs文件路径
	 * @throws IOException
	 */
	public static void getFileFromHDFS(String globalHdfsPath, String localPath, String hdfsPath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path hdfsFile = new Path(hdfsPath);
		File f = new File(localPath);
		if (f.exists()) {
			System.out.println(localPath + "文件已存在");
			Integer i = 0;
			while (true) {
				f = new File(localPath + "_" + i.toString());
				if (!f.exists()) {
					localPath = localPath + "_" + i.toString();
					System.out.println("文件被重命名为" + localPath);
					break;
				}
				i++;
			}
		}
		Path localFile = new Path(localPath);
		fs.copyToLocalFile(hdfsFile, localFile);
		fs.close();
	}

	/**
	 * 从hdfs读取文件全部内容
	 *
	 * @param globalHdfsPath hdfs根目录地址
	 * @param hdfsPath       hdfs文件路径
	 * @return String形式的文件全部内容
	 * @throws IOException
	 */
	public static String getContentFromHDFS(String globalHdfsPath, String hdfsPath) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", globalHdfsPath);
		FileSystem fs = FileSystem.get(conf);
		Path file = new Path(hdfsPath);
		if (fs.exists(file)) {
			FSDataInputStream inStream = fs.open(file);
			String re = new String(inStream.readAllBytes());
			return re;
		}
		return null;
	}
}
