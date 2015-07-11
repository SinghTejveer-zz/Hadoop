package Cloudwick.Temperature1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  private final int MinCharsInString = 15;
  private final int ExpectedColumnCount = 7;
  private final int TempIndex = 2;
  private final int YearIndex = 0;

  String SPLIT_REGEX = " +";

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String Line = value.toString();
    if (Line == null || Line.isEmpty()) {
      return;
    }

    if (Line.length() < MinCharsInString) {
      return;
    }

    String[] recordSplits = Line.trim().split(SPLIT_REGEX);

    if (recordSplits.length == ExpectedColumnCount) {

      String Year = recordSplits[YearIndex];

      Double airTemperature;

      try {

        airTemperature = Double.parseDouble(recordSplits[TempIndex]);

        context.write(new Text(Year), new DoubleWritable(airTemperature));

      } catch (NumberFormatException nfe) {

        return;
      }
    }
  }
}
