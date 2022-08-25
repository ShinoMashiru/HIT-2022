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

public  class DoneMapper extends Mapper<LongWritable, Text, Text, Traveler> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Traveler tmp = new Traveler(value.toString());
			
			if(8.1461259 <= tmp.longitude && tmp.longitude <= 11.1993265 && 56.5824856 <= tmp.latitude && tmp.latitude <= 57.750511){
				context.write(new Text(" "), tmp);
			}
		}
	}
