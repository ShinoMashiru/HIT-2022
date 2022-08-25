import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class SampleMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	// 以career为key划分，分层

	Text key_career = new Text(value.toString().split("\\|")[10].toLowerCase());
	context.write(key_career, value);

	}
}
