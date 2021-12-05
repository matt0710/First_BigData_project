package FP.FirstProject;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EducationMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable> {

    boolean check = true;
    String regex = "[0-9]+";
    Pattern p = Pattern.compile(regex);

    public void map (LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {

        String[] row = value.toString().split(";"); //we collect data into the string vector "row"

        //with the following check, we don't process the first row of the dataset, which contains the title of each field of the file
        if (check) {
            check = false;
            return;
        }

        if (row.length!=24) return; //we verify if the number of items in the row vector is 24: if it is false, it means that an error
        //occurs in the data insertion process in the file
        Matcher m = p.matcher(row[23]);
        if (!m.matches()) return;//we check if the field "Participants age" is composed by only digits or not, in order to avoid
        //exceptions
        if (row[6].equals("? Unknown ?")) return; //we check if the field of education is unknown: if it is true we discard the row

        context.write(new Text(row[6]), new IntWritable(Integer.parseInt(row[23])));
    }
}
