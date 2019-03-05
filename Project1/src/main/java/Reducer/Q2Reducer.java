package Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q2Reducer  extends Reducer<Text, FloatWritable, Text, FloatWritable>{
	@Override
	protected void reduce(Text arg0, Iterable<FloatWritable> arg1,
			Reducer<Text, FloatWritable, Text, FloatWritable>.Context arg2) throws IOException, InterruptedException {
		List<Float> list = new ArrayList<Float>();
		for (FloatWritable num: arg1) {
			list.add(num.get());
		}
		if(list.size()>=2) {
			int num=1;
			float totalIncreases=0;
			for(int i =1; i<list.size();i++) {
				 totalIncreases= totalIncreases+  list.get(list.size()-1)-list.get(0);
				num++;
			}
			float avgIncrease=Math.abs(totalIncreases/num);
			arg2.write(arg0, new FloatWritable(avgIncrease));
		}
		
	}
}
