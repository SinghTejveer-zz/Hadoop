package Cloudwick.Income;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class SecondarySortBasicGroupingComparator extends WritableComparator {
  protected SecondarySortBasicGroupingComparator() {
    super(CompositeKeyWrite.class, true);
  }

  @SuppressWarnings("unused")
  private Logger logger = Logger.getLogger("FilterMapper");

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    CompositeKeyWrite key1 = (CompositeKeyWrite) w1;
    CompositeKeyWrite key2 = (CompositeKeyWrite) w2;

    return key1.getIncome().compareTo(key2.getIncome());
  }
}