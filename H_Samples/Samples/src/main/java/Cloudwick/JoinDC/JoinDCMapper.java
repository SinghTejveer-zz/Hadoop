package Cloudwick.JoinDC;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinDCMapper extends Mapper<Object, Text, Text, Text> {

  Path[] cachefiles = new Path[0]; // To store the path of lookup files
  List<String> Artists = new ArrayList<String>();// To store the data of lookup
                                                 // files

  @Override
  public void setup(Context context)

  {
    Configuration conf = context.getConfiguration();

    try {

      cachefiles = DistributedCache.getLocalCacheFiles(conf);

      reader = new BufferedReader(new FileReader(cachefiles[0].toString()));

      String line1;

      while ((line1 = reader.readLine()) != null) {

        Artists.add(line1); // Data of lookup files get stored in list object
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  private final int StateIndex = 3;
  // String seek = "night";
  String seperator = "<SEP>";
  private BufferedReader reader;

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    String[] splits = line.toString().split(seperator);

    if (splits.length == StateIndex + 1) {

      // Boolean containsSearchword =
      // splits[StateIndex].toLowerCase().contains(seek);

      String Trackid = splits[StateIndex - 3];
      // String Songid = splits[StateIndex - 2];
      String Artistname = splits[StateIndex - 1];
      String Title = splits[StateIndex];

      // Filter
      // if (containsSearchword) {
      for (String e : Artists) {
        // for (int i = 0; i <= Artists.size(); i++) {
        // String e = Artists.get(i);
        //
        String[] listLine = e.toString().split(seperator);
        //

        if (listLine.length > 0) {
          String Track = listLine[2];

          if (Trackid.equals(Track)) {
            context.write(new Text(Trackid), new Text(listLine[0] + "\t"
                + Artistname + "\t" + Title));
            // }
          }
        }
      }
    }
  }
}