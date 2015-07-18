package Cloudwick.Income;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import Cloudwick.Income.IncomeDriver.COUNTERS;

public class CountryIncomeReducer
    extends
    Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {

  @SuppressWarnings("unused")
  private Logger logger = Logger.getLogger("FilterMapper");

  @Override
  public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values,
      Context context) throws IOException, InterruptedException {

    long count = context.getCounter(COUNTERS.RECORDS).getValue();

    if (count == 10) {
      return; // Displaying only top 10 and lowest 10 countries
    }

    for (@SuppressWarnings("unused")
    NullWritable value : values) {

      context.getCounter(COUNTERS.RECORDS).increment(1);

      context.write(key, NullWritable.get());

    }

  }
}