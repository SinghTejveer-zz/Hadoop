/*
MR job for Filter the records for the songs with title containing "night"
Then it sends output of first mappaer1 as an input to the other mapper2.
 */

package Cloudwick.JobChain;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChainMapper1 extends Mapper<Object, Text, Text, Text> {

  private final int TrackIndex = 0;
  private final int ArtistIndex = 2;
  private final int TitleIndex = 3;
  private final int ExpectedSplitsLength = 4;

  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      return;
    }

    String[] recordSplits = line.toString().split(seperator);

    // Checking if the length of recordSplits is exactly what we are finding
    if (recordSplits.length == ExpectedSplitsLength) {

      // Searching for the keyword
      Boolean containsSearchword =
          recordSplits[TitleIndex].toLowerCase().contains(seek);

      String Trackid = recordSplits[TrackIndex];
      String Artistname = recordSplits[ArtistIndex];
      String Title = recordSplits[TitleIndex];

      // Map if keyword found
      if (containsSearchword)
 {
        context.write(new Text(" "), new Text(Trackid + "\t" + Artistname
            + "\t" + Title));
      }
    }
  }
}
