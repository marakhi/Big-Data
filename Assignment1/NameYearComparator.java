//package uis.bigdataclass.MaxBirthtyear;
import org.apache.hadoop.io.*;
/**
 * A comparator for the KeyPair class that compairs by lastname and then birth year Desc
 * This comparator is used for sorting the keypair by the mapreduce framework
 * @author Ellie Buxton
 *
 */
public class NameYearComparator extends WritableComparator {
	
	public NameYearComparator() {
		super(KeyPair.class, true);	
	}
	
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		int c = key1.getmainkey().compareTo(key2.getmainkey());
		if (c ==0)
			return key1.getuniqueDestination().compareTo(key2.getuniqueDestination());
		else
			return c;
	}
}
