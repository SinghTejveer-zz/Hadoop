package Cloudwick.Income;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import Cloudwick.Income.IncomeDriver.COUNTERS;

public class CountryIncomeMapper extends
    Mapper<Object, Text, CompositeKeyWritable, NullWritable> {

  private Logger logger = Logger.getLogger("FilterMapper");

  private final int incomeIndex = 54;
  private final int countryIndex = 0;
  private final int lenIndex = 58;

  String seperator = ",";

  public void map(Object key, Text line, Context context) throws IOException,
      InterruptedException {

    if (line == null | !!line.toString().isEmpty()) {
      logger.info("null found.");
      context.getCounter(COUNTERS.NULL_OR_EMPTY).increment(1);
      return;
    }
    if (line.toString().contains(
        "Adjusted net national income per capita (current US$)")) {
      String[] recordSplits = line.toString().split(seperator);

      logger.info("The data has been splitted.");

      if (recordSplits.length == lenIndex) {

        String countryName = recordSplits[countryIndex];
        try {

          double income = Double.parseDouble(recordSplits[incomeIndex]);

          // Setting the values for composite Key Writable
          CompositeKeyWritable k = new CompositeKeyWritable();
          k.setCountryName(countryName);
          k.setIncome(income);

          context.write(k, NullWritable.get());

        } catch (NumberFormatException nfe) {

          logger.info("The value of income is in wrong format. " + countryName);
          context.getCounter(COUNTERS.MISSING_FIELDS_RECORD_COUNT).increment(1);
          return;
        }

      }
    }
  }
}