//package uis.bigdataclass.MaxBirthtyear;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;


public class KeyPair implements WritableComparable<KeyPair>{
 
	//the key pair holds the lastname and birthyear
	private Text mainkey;
	private Text uniqueDestination;
	
	//The defaule constructor
	public KeyPair()
	{
		mainkey = new Text();
		uniqueDestination = new Text();
	}
	
	//constructor, initializing the lastname and birthyear
	public KeyPair(String key, String uniqueDest)
	{
		mainkey = new Text(key);
		uniqueDestination= new Text(uniqueDest);
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		mainkey.readFields(in);
		uniqueDestination.readFields(in);
	}

	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		mainkey.write(out);
		uniqueDestination.write(out);
	}


	public int compareTo(KeyPair otherPair) {
		// TODO Auto-generated method stub
		int c= mainkey.compareTo(otherPair.mainkey);
		if (c!=0)
			return c;
		else
			return uniqueDestination.compareTo(otherPair.uniqueDestination);
	}
	
	//the Getter and setter methods
	public Text getmainkey() {
		return mainkey;
	}

	public void setLastname(Text mainkey) {
		this.mainkey = mainkey;
	}

	public Text getuniqueDestination() {
		return uniqueDestination;
	}

	public void setuniqueDestination(Text uniqueDestination) {
		this.uniqueDestination = uniqueDestination;
	}

}
