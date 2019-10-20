

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
  Mapper<LongWritable, Text, IntWritable, Text> {

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString(), "|");
    
    int keyOut;
      keyOut = Integer.parseInt(itr.nextToken());
      IntWritable iw = new IntWritable(keyOut);
   
     
      context.write(iw, value);
//      while (itr.hasMoreTokens()) {
//      		}
  }
}