import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;


public class HdfsGet {
  public static void main(String[] args) throws Exception {
//	  long startTime = System.currentTimeMillis();
	  
    Configuration conf = new Configuration(); 
    FileSystem fs = FileSystem.get(conf);
    

    Path filenamePath1 = new Path("lineitem5G.tbl");
   // Path filenamePath2 = new Path("PAPER/dataFile.txt");
	  long startTime = System.currentTimeMillis();
    
    byte[] buf = new byte[1000];
    FSDataInputStream fout = fs.open(filenamePath1);
    fout.skip(500000000);
    fout.skip(500000000);
    fout.read(buf, 0, 715);
            //Print to screen
    String s1 = new String(buf,0,715);
    
    System.out.println(s1);
    System.out.println("gogogo3");
    long endTime   = System.currentTimeMillis();
//    fout.skip(500000);
//    
//    fout.read(buf, 0, 129);
//    String s2 = new String(buf,0,129);
//    Path filenamePath2 = new Path("PAPER/dataFile.txt");
//    System.out.println(s2);
//    
//    System.out.println("stkpl");
//    

    
    fout.close();
    
// FSDataInputStream fout2 = fs.open(filenamePath2);
//    
//    fout2.read(buf, 0, 715);
//            //Print to screen
//    String s3 = new String(buf,0,715);
//    System.out.println(s3);
//    
//    fout2.skip(500000);
//    
//    fout2.read(buf, 0, 129);
//    String s4 = new String(buf,0,129);
//    System.out.println(s4);
//    
//    //System.out.println("stkpl");t");
//    Path filenamePath2 = new Path("PAPER/dataFile.txt");
//    Path filenamePath2 = new Path("PAPER/dataFile.txt");
//   
//    fout2.close();
//    
    
    //System.out.println("stkpl");
	//  long endTime   = System.currentTimeMillis();
	  
	  long totalTime = endTime - startTime;
	  System.out.println("totalTime : " + totalTime);
  }
}