import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MostFrequentDestReducer extends
    Reducer<KeyPair, Text, Text, Text> {
	
	Text textValue = new Text();
  public void reduce(KeyPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    int max = 0;
    String key1 = null;
    String temp = " ";
    String Dest = " ";
    //System.out.println(values);
    
		for (Text value : values) {
			
			String s = value.toString();
			//temp = s + temp;
			//System.out.println("Marakhi :"+ s + " " + temp + "end");
//			if (temp == s){
//				System.out.println("Marakhi : true");
//			}else {
//				System.out.println("Marakhi : false");
//			}
			if (temp.equals(s)){
				sum = sum + 1;
				System.out.println("Marakhi :"+ sum);
			}else if (temp.equals(" ")){
				temp = s;
				sum = 1;
			}else {
				if (sum > max){
					max = sum;
					Dest = temp;
					//System.out.println("Marakhi :"+ max + " " + Dest);
					sum = 1;
					temp = s;
				}else{
					temp = s;
					sum = 1;
				}
			}
			}
		if (sum > max){
			max = sum;
			Dest = temp;
		}
		String val = Dest + " " + max;
			//textValue.set(val);
			context.write(key.getmainkey(), new Text(val));

  }
}
