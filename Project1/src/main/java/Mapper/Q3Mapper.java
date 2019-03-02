package Mapper;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q3Mapper  extends Mapper<LongWritable, Text, Text, FloatWritable> {
	@Override
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if(line.contains("United States") && line.contains("Employment to population ratio, 15+, female")){
			String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			String country= tokens[0].substring(1, (tokens[0].length()-1));
			for(int i =0; i<tokens.length; i++) {
				if(i>=40 && tokens[i].length()>=2 && Character.isDigit(tokens[i].charAt(1))) {
					float f= Float.parseFloat(tokens[i].substring(1, (tokens[i].length()-1)));
					context.write(new Text(country), new FloatWritable(f));
				}
			}
		}
	}
}