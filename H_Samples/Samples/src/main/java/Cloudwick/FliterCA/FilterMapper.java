package Cloudwick.FliterCA;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<Object, Text, Text, Text> {

  // For CSV file StateProvince.cs

  private final int StateIndex = 1;

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    String[] csv = line.toString().split("\t");

    if (csv.length > 1) {

      // "<Space>apple and banana " => "apple and banana" (Remove Spaces)
      String trimmed = csv[StateIndex].trim();

      byte[] foo = trimmed.getBytes();

      byte[] bar = new byte[2];
      bar[0] = foo[0];
      bar[1] = foo[2];

      if (new String(bar).compareToIgnoreCase("CA") == 0) {

        // if (trimmed == "CA") {
        context.write(new Text(), new Text(line));
      }
    }
  }
}