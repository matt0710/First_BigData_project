package FP.FirstProject;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class MaxParticipantsReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce (Text text, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {

        String[] row = new String[2];
        HashMap<String, Integer> map = new HashMap<>();

        for (Text t : iterable) {
            row = t.toString().split(";"); //we fill the row vector with the values encoded in the map function, such as the
            //receiving organization code and the number of participants
            if (map.containsKey(row[0]))
                map.replace(row[0], Integer.parseInt(row[1]) + map.get(row[0])); //if the receiving organization code is already present in
                //the map, we replace the entry putting as value the sum of the current value and the new number of participants
            else
                map.put(row[0], Integer.parseInt(row[1]));
        }

        Integer max = Collections.max(map.values()); //we evaluate the max number of participants value associated with the key "text"

        for (Map.Entry<String, Integer> entry: map.entrySet())
        {
            if (max == entry.getValue()) {
                String receivingMax = entry.getKey();
                context.write(text, new Text(receivingMax)); //we add to context the pair (sending_organization_code, receiving_organization_code)
                //for which the receiving organization is the one with the biggest number of participants just evaluated
            }
        }
    }
}