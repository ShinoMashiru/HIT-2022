import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
	

public class DoneReducer extends Reducer<Text, Traveler, NullWritable, Text> {
		@Override
		protected void reduce(Text key, Iterable<Traveler> values, Context context) throws IOException, InterruptedException {
			List<Traveler> pool = new ArrayList<>();
		
			
			Double minRating = Double.POSITIVE_INFINITY, 
			maxRating = Double.NEGATIVE_INFINITY, 
			ratingSum = 0.0, 
			tot = 0.0;

			Map<String, Double> nationCareerIncome = new HashMap<>();
			Map<String, Integer> nationCareerCount = new HashMap<>();
			for(Traveler value : values) {
				pool.add(value.clone());
				
				
				if(value.rating <= 100.0 && value.rating >= 0.0) {
					minRating = Double.min(minRating, value.rating);
					maxRating = Double.max(maxRating, value.rating);
					tot++;
					ratingSum += value.rating;
				}
				 
				// 若不是空值, 则对国家zhiye平均工资进行更新
				if(value.user_income!=-1.0){
					double preTot = nationCareerIncome.get(value.user_nationality+value.user_career)==null ? 0.0
									: nationCareerIncome.get(value.user_nationality+value.user_career);
					nationCareerIncome.put(value.user_nationality+value.user_career,value.user_income + preTot);// 更新总工资数, 为了计算平均工资
					int count= preTot == 0.0 ? 0 : nationCareerCount.get(value.user_nationality+value.user_career);
					nationCareerCount.put(value.user_nationality+value.user_career, count + 1);
				}
			}
			Double avgRating = ratingSum / tot;
			for(Traveler p : pool) {
				// 对收入的空值进行补充
				if(p.user_income == -1.0){
					p.user_income = nationCareerIncome.get(p.user_nationality+p.user_career) / nationCareerCount.get(p.user_nationality+p.user_career);
				}
				// 对离群或者空值进行修改
				if(p.rating < 0 || p.rating > 100){
					p.rating = avgRating;
				}
				// 计算归一化后的rating
				p.rating = (p.rating - minRating) / (maxRating - minRating);
			}
			for(Traveler each : pool){
				context.write(NullWritable.get(), new Text(each.toString()));
			}
		}
	}

