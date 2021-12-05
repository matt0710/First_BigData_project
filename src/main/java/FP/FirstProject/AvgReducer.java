package FP.FirstProject;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce (Text text, Iterable<FloatWritable> iterable, Context context) throws IOException, InterruptedException {

        float avgValue = 0;
        float sum = 0;
        float count = 0;

        for (FloatWritable it : iterable) {
        	sum += it.get();
        	count++;
        }
        //after the loop we simply evaluate the sum of all the ages related to the key "text"
        
        avgValue = (sum/ count); //here we process the average of participant's age

        context.write(text, new FloatWritable(avgValue));
    }
}
