

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
  private IntWritable key = new IntWritable();

  public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int sum = 0;
    for (Text val : values) {
    	context.write(key, val);
       }
   
    
  }
}