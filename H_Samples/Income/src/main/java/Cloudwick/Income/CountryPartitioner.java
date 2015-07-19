package Cloudwick.Income;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CountryPartitioner extends
    Partitioner<CompositeKeyWrite, NullWritable> {

  @Override
  public int getPartition(CompositeKeyWrite key, NullWritable value,
      int numReduceTasks) {

    return (key.getCountryName().hashCode() % numReduceTasks);
  }
}