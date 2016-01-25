package org.mdp.hadoop.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join {
	
	public static void main(String[] args) throws Exception {
		
		Path page = new Path(args[0]);
	    Path imagelinks = new Path(args[1]);
	    Path output = new Path(args[2]);

	    Job job = new Job(new Configuration(), "Join");
	    job.setJarByClass(Join.class);
	    
	    MultipleInputs.addInputPath(job, page, TextInputFormat.class, PageMapper.class);
	    MultipleInputs.addInputPath(job, imagelinks, TextInputFormat.class, ImagelinksMapper.class);
	    
	    job.setReducerClass(ReduceSideJoin.class);
	    job.setNumReduceTasks(1);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileOutputFormat.setOutputPath(job, output);
		
	    if (job.waitForCompletion(true)) return;
	    else throw new Exception("First Job Failed");
	}	
	
	public static String replaceN(String raw, int n, boolean startLeft){
		int i;
		int replaced = 0;
		char[] charArray = raw.toCharArray();
		
		if (startLeft){
			for (i=0; i<charArray.length; i++){
				if (charArray[i]==','){
					charArray[i]=' ';
					replaced++;
					
					if (replaced==n) break;
				}	
			}
		}
		else{
			for (i=charArray.length-1; i>=0; i--){
				if (charArray[i]==','){
					charArray[i]=' ';
					replaced++;
					
					if (replaced==n) break;
				}	
			}
		}
		return new String(charArray);
	}

}
