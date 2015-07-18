package Cloudwick.Income2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class IncomeMapper extends Mapper<Object, Text, Text, DoubleWritable> {

  private Logger logger = Logger.getLogger("Income_Mapper");

  private final int incomeIndex = 54;
  private final int countryIndex = 0;
  private final int expectedLenIndex = 58;

  String seek = "Adjusted net national income (current US$)";
  String seperator = ",";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null) {
      logger.info("Null record found.");
      return;
    }
    if (line.toString().contains(
        "Adjusted net national income per capita (current US$)")) {
      String[] recordSplits = line.toString().split(seperator);

      logger.info("The data has been splitted.");

      if (recordSplits.length == expectedLenIndex) {

        String countryName = recordSplits[countryIndex];
        try {

          double income = Double.parseDouble(recordSplits[incomeIndex]);

          context.write(new Text(countryName), new DoubleWritable(income));

        } catch (NumberFormatException nfe) {

          logger.info("The value of income is in wrong format.");

          return;
        }

      }
    }
  }
}