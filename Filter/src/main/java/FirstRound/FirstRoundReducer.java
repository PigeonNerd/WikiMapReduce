package FirstRound;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstRoundReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	private LongWritable SUM = new LongWritable();
	@Override
	  protected void reduce(Text title, Iterable<LongWritable> views, Context context) 
			  throws IOException, InterruptedException {
		long sum = 0;
		for(LongWritable view : views) {
			sum += view.get();
		}
		SUM.set(sum);
		context.write(title, SUM);
	}
}
