package org.mdp.hadoop.cli;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideJoin extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		String pageName = "UNKNOWN";
		String imageName = "UNKNOWN";
		for (Text pageOrImageName: values){
			if (pageOrImageName.toString().startsWith("PAG-")){
				pageName = pageOrImageName.toString().substring(4);
				break;
			}
		}
		for (Text pageOrImageName: values){
			if (pageOrImageName.toString().startsWith("IMG-")){
				imageName = pageOrImageName.toString().substring(4);
				context.write(new Text(imageName), new Text(pageName));
			}
		}
	}
}
