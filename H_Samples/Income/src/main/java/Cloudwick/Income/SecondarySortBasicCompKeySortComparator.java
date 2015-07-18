package Cloudwick.Income;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class SecondarySortBasicCompKeySortComparator extends WritableComparator {

  protected SecondarySortBasicCompKeySortComparator() {
    super(CompositeKeyWritable.class, true);
  }

  @SuppressWarnings("unused")
  private Logger logger = Logger.getLogger("FilterMapper");

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
    CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

    return -key1.getIncome().compareTo(key2.getIncome());

    // If the minus is taken out, the values will be in
    // ascending order
  }
}