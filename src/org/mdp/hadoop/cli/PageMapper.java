package org.mdp.hadoop.cli;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {

		String[] rawWords = value.toString().split(" "); //Separate line by " "
    	//System.out.println(rawWords[0]);
    	if (rawWords[0].equals("INSERT")){
    		
    		//Get tripletArray, each triplet contains page_id,image_name,number
	    	String rawTriplets = rawWords[4].substring(1, rawWords[4].length()-2);
	    	String[] tripletArray = rawTriplets.split("\\),\\(");
	    	
	    	int i;
	    	for (i=0; i<tripletArray.length; i++){

	    		//Replace first 2 and last 10 commas
	    		String triplet = Join.replaceN(tripletArray[i],2,true);
	    		triplet = Join.replaceN(triplet,10,false);
	    		
	    		//Split
	    		String[] data = triplet.split(" ");
	    		
	    		if (data.length!=13)
	    			System.out.println("Error: " + triplet);
	    		
	    		
	    		Text pageId = new Text(data[0]);
	    		Text pageTitle = new Text("PAG-" + data[2].substring(1,data[2].length()-1));
	    		context.write(pageId, pageTitle);
	    	}
    	}
	}
	
	/*
	public static void main (String args[]) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("page1.txt"));
		try {
		    StringBuilder sb = new StringBuilder();
		    String line = br.readLine();

		    while (line != null) {
		    	String[] rawWords = line.split(" "); //Separate line by " "
		    	//System.out.println(rawWords[0]);
		    	if (rawWords[0].equals("INSERT")){
		    		
			    	//Get tripletArray, each triplet contains
		    		//page_id,page_namespace,page_title,page_restrictions,page_counter,page_is_redirect
			    	String rawTriplets = rawWords[4].substring(1, rawWords[4].length()-2);
			    	String[] tripletArray = rawTriplets.split("\\)\\,\\(");
			    	
			    	int i;
			    	for (i=0; i<tripletArray.length; i++){
			    		String triplet = Join.replaceN(tripletArray[i],2,true);
			    		triplet = Join.replaceN(triplet,10,false);
			    		//System.out.println(triplet); //Insert
			    		
			    		String[] value = triplet.split(" ");
			    		if (value.length!=13)
			    			System.out.println("Error: " + tripletArray[i]); //Insert
			    	}
		    	}
		        line = br.readLine();
		    }
		    String everything = sb.toString();
		} finally {
		    br.close();
		}
	}/**/

}
