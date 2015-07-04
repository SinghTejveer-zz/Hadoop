package Cloudwick.Samples;

import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

  // private final int StateIndex = 1;;

  public void map(Object key, Text value, Context context) throws IOException,
      InterruptedException {

    // String[] csv = line.toString().split("\t");
    // if (csv.length > 1) {
    //
    // String trimmed = csv[StateIndex].trim();
    // byte[] foo = trimmed.getBytes();
    // byte[] bar = new byte[2];
    // bar[0] = foo[0];
    // bar[1] = foo[2];
    //
    // // String bytes = trimmed + " ";
    // // if (csv[] == "CA") {
    //
    // if (new String(bar) == "CA") {
    //
    // context.write(new Text(""), new Text(line));
    //
    String seperater = "<SEP>";
    int len = 4;
    int index = 3;
    String[] splitted = value.toString().split(seperater);
    String searchWord = "night";

    if (splitted.length >= len) {
      Boolean containsSearchWord =
          splitted[index].toLowerCase().contains(searchWord);

      if (containsSearchWord) {
        context.write(new Text(), new Text(value));
      }
    }
  }
}

// }
