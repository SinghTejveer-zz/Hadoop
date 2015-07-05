/*
Takes the output from the mapper1 as its input and checks if it contains keyword 
"black".
The output will be only those records contain black & night.
 */

package Cloudwick.JobChain;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChainMapper2 extends Mapper<Object, Text, Text, Text> {

  private final int TitleIndex = 3;
  private final int TrackIndex = 2;
  private final int ArtistIndex = 1;

  String seek = "black";
  String outputSep = "\t";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      return;
    }

    String[] splits = line.toString().split(outputSep);

    // Checking if the length of recordSplits is exactly what we are finding
    if (splits.length == TitleIndex + 1) {

      String Trackid = splits[TrackIndex];
      String Artistname = splits[ArtistIndex];
      String Title = splits[TitleIndex];

      // Searching for the keyword
      Boolean containsSearchword = Title.toLowerCase().contains(seek);

      // Map if keyword found
      if (containsSearchword)
        context.write(new Text(" "), new Text(Trackid + outputSep + Artistname
            + outputSep + Title));

    }
  }
}
