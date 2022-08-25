public class Util {
	/**
	 * 计算两个点之间的欧氏距离
	 */
	public static Double getDistance(String o1, String o2) {
		String[] point1 = o1.split(",");
		String[] point2 = o2.split(",");
		double dis = 0;
		for(int i=0;i<20;i++){
			double x1 = Double.parseDouble(point1[i]);
			double x2 = Double.parseDouble(point2[i]);
			dis += (x1 - x2) * (x1 - x2);
		}
		return dis;
	}
}
