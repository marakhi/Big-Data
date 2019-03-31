import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MostFrequentDestMapper extends
    Mapper<LongWritable, Text, KeyPair, Text> {
	Text textKey = new Text();
	Text textValue = new Text();
	
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
   ///Replacing all digits and punctuation with an empty string
	  String line = value.toString().replaceAll("\\p{Punct}|\\d", "").toLowerCase();
   //Extracting the words
	 String[]columns = value.toString().split(",");
	 String uniqueCarrier= columns[8];
	 String uniqueOrigin= columns[16];
	 String uniqueDestination= columns[17];

	 if (uniqueDestination.equals("NA") || uniqueDestination.equals("Dest") || uniqueDestination.equals(" ") || uniqueOrigin.equals(" ") || uniqueOrigin.equals(" ")|| uniqueCarrier.equals(" ")||uniqueCarrier.equals("NA")){
		 
	 }else{
		//int deptDelay = Integer.parseInt(columns[15]);
		 int i = 1;
		 //String s = uniqueDestination;
		 //String v= s+","+String.valueOf(i);
		 textValue.set(uniqueDestination);
		 String mainkey = uniqueCarrier+" "+uniqueOrigin;
         //context.write(new Text(uniqueCarrier+" "+uniqueOrigin), textValue);
         context.write(new KeyPair(mainkey, uniqueDestination), textValue);
	 }
    }
  }

