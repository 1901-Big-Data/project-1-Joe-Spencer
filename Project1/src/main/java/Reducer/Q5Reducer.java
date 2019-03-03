package Reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q5Reducer  extends Reducer<Text, FloatWritable, Text, FloatWritable>{
	@Override
	protected void reduce(Text arg0, Iterable<FloatWritable> arg1,
			Reducer<Text, FloatWritable, Text, FloatWritable>.Context arg2) throws IOException, InterruptedException {
		float total = 0;
		int size=0;
		
		for (FloatWritable num: arg1) {
			total += num.get();
			size++;
		}
		float average = total/size;
		if(average>=25) {
			arg2.write(arg0, new FloatWritable(average));
		}
		
	}
}
