package FP.FirstProject;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class EducationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private HashMap<String, Integer> map ;

    public void setup (Context context) {
        map = new HashMap<>(); //with setup method we initialize the map: the latter will contain all the field of education and the
        //number of participants
    }


    public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for (IntWritable val: values) {
            if (map.containsKey(key.toString())) {
                map.replace(key.toString(), val.get() + map.get(key.toString()));
            }
            else {
                map.put(key.toString(), val.get());
            }
        }
        //if the map already contains a certain field of education we replace the entry related to this one with another which value is the
        //sum of the previous one and the new. If the map doesn't contain the key, we simply put it into the data structure
    }

    public void cleanup (Context context) throws IOException, InterruptedException {

        //cleanup method is called once all reduce phases have been done.
        List<Integer> participants = new ArrayList<>(map.values());
        Collections.sort(participants, Collections.reverseOrder()); //we first put the map values into an arrayList and then we sort
        //in descending order the latter.

        if (participants.size() < 5) //we do this check in order to avoid IndexOutOfBoundException
            for (int i=0; i<participants.size(); i++) {
                for (Map.Entry<String,Integer> entry : map.entrySet()) {
                    if (participants.get(i) == entry.getValue()) { //if the entry value is equal to the ith-item in the ordered list, we
                        //add it to the context
                        context.write(new Text(entry.getKey()), new IntWritable(participants.get(i)));
                    }
                }
            }
        else
            for (int i=0; i<5; i++) {
                for (Map.Entry<String,Integer> entry : map.entrySet()) {
                    if (participants.get(i) == entry.getValue()) {
                        context.write(new Text(entry.getKey()), new IntWritable(participants.get(i)));
                    }
                }
            }
    }
}