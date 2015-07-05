package Cloudwick.Join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper2 extends Mapper<Object, Text, Text, Text> {

  private final int TitleIndex = 3;
  private final int TrackIdIndex = 2;
  private final int ArtistIdIndex = 0;
  private final int ExpectedSplitLength = 4;

  String seek = "night";
  String seperator = "<SEP>";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      return;
    }

    String[] splits = line.toString().split(seperator);

    if (splits.length == ExpectedSplitLength) {

      // Search for the keyword
      Boolean containsSearchword =
          splits[TitleIndex].toLowerCase().contains(seek);

      String ArtistId = splits[ArtistIdIndex];
      String TrackId = splits[TrackIdIndex];

      // Map if keyword found
      if (containsSearchword)
        context.write(new Text(TrackId), new Text(ArtistId));

    }
  }
}
