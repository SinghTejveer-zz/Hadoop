package Cloudwick.JobChain;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChainMapper2 extends Mapper<Object, Text, Text, Text> {

  private final int StateIndex = 3;
  String seek = "black";
  String seperator = "\t";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    String[] splits = line.toString().split(seperator);

    if (splits.length == StateIndex + 1) {

      String Trackid = splits[StateIndex - 2];
      String Artistname = splits[StateIndex - 1];
      String Title = splits[StateIndex];

      Boolean containsSearchword = Title.toLowerCase().contains(seek);

      // Filter
      if (containsSearchword)
        context.write(new Text(" "), new Text(Trackid + "\t" + Artistname
            + "\t" + Title));

    }
  }
}
