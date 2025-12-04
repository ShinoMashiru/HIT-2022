import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Convert {
	/**
	 * 日期转换静态函数，将输入的日期转换为yyyy-mm-dd格式
	 * @param srcDate 原始日期字符串
	 * @return 转换后的日期字符串
	 */
	public static String convertDate(String srcDate){
		String re;
		List<String> month = new ArrayList<>(Arrays.asList("january", "february", "march", "april", "may", "june",
														   "july","august", "september", "october", "november",
													       "december"));
		String y, m, d;
		srcDate = srcDate.trim();//先删除起止的空格
		if(Character.isLowerCase(srcDate.charAt(0)) || Character.isUpperCase(srcDate.charAt(0))){// 检测 Month day,year格式的输入字符串
			//空格分割出月份，逗号分割出年份和天
			String[] tmpDate = srcDate.split(" ");
			String tmpMonth = tmpDate[0].toLowerCase();
			d = tmpDate[1].split(",")[0].trim();
			y = tmpDate[1].split(",")[1].trim();
			m = String.valueOf(month.indexOf(tmpMonth) + 1);
			if(m.length() == 1){//一位数补零
				m = "0" + m;
			}
			if(d.length() == 1){//一位数补零
				d = "0" + d;
			}
			re = y + "-" + m + "-" + d;
		}else if(srcDate.contains("/")){// 检测用/划分的日期字符串
			String[] tmpDate = srcDate.split("/");// 直接使用/进行分割
			y=tmpDate[0].trim();
			m=tmpDate[1].trim();
			d=tmpDate[2].trim();
			re = y + "-" + m + "-" + d;
		}else{
			re = srcDate;
		}
		return re;
	}

	/**
	 * 温度转换函数
	 * @param srcTemp 输入的原始温度（摄氏度/华氏度）
	 * @return 摄氏温度字符串
	 */
	public static String convertTemperature(String srcTemp){
		String re;
		if(srcTemp.charAt(srcTemp.length()-1) != '℃'){
			Double tmp = Double.valueOf(srcTemp.substring(0, srcTemp.length() - 2));
			re = String.format("%.1f", (tmp - 32.0) / 1.8) + "℃";//检测出华氏度使用公式进行转换。
		}else{
			re = srcTemp;
		}
		return re;
	}
}
