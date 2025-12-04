import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SampleReducer extends Reducer<Text, Text, NullWritable, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for(Text value : values) {

			//采样率0.001，根据key值分层抽样
			if(Math.random() <= 0.0001)
				context.write(NullWritable.get(), new Text(value.toString()));
		}	
	}
}
