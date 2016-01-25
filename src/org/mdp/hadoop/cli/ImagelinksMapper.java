package org.mdp.hadoop.cli;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ImagelinksMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		String[] rawWords = value.toString().split(" "); //Separate line by " "
    	//System.out.println(rawWords[0]);
    	if (rawWords[0].equals("INSERT")){
	    	
	    	//Get tripletArray, each triplet contains page_id,image_name,number
	    	String rawTriplets = rawWords[4].substring(1, rawWords[4].length()-2);
	    	String[] tripletArray = rawTriplets.split("(?<=[0-9])\\),\\(");
	    	
	    	int i;
	    	for (i=0; i<tripletArray.length; i++){
	    		String triplet = Join.replaceN(tripletArray[i],1,true);
	    		triplet = Join.replaceN(triplet,1,false)
	    				.replace("\\", "");
	    		//System.out.println(triplet);
	    		
	    		String[] data = triplet.split(" ");
	    		
	    		Text pageId = new Text(data[0]);
	    		Text imageName = new Text("IMG-" + data[1].substring(1,data[1].length()-1));
	    		context.write(pageId, imageName);
	    	}
    	}
	}
	/*
	public static void main (String args[]) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("enwiki-20151201-imagelinks.sql"));
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
			    	String[] tripletArray = rawTriplets.split("(?<=[0-9])\\)\\,\\(");
			    	
			    	int i;
			    	for (i=0; i<tripletArray.length; i++){
			    		String triplet = Join.replaceN(tripletArray[i],1,true);
			    		triplet = Join.replaceN(triplet,1,false);
			    		//System.out.println(triplet); //Insert
			    		
			    		String[] value = triplet.split(" ");
			    		if (value.length!=3)
			    			System.out.println("Error: " + tripletArray[i]); //Insert
			    		try {
			    			String imgName = value[1].substring(1,value[1].length()-1);
			    		} catch (Exception e){
			    			System.out.println("Error with image name: " + value[1]);
			    			String imgName = value[1];
			    		}
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
