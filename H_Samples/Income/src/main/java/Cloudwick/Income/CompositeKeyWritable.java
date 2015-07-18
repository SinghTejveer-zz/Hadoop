package Cloudwick.Income;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

@SuppressWarnings("rawtypes")
public class CompositeKeyWritable implements WritableComparable {

  private String countryName;
  private Double income;

  public CompositeKeyWritable() {
  }

  public CompositeKeyWritable(String countryName, Double income) {
    this.countryName = countryName;
    this.income = income;
  }

  @Override
  public String toString() {
    return (new StringBuilder().append(countryName).append("\t").append(income))
        .toString();
  }

  public void readFields(DataInput dataInput) throws IOException {
    countryName = WritableUtils.readString(dataInput);
    income = dataInput.readDouble();
  }

  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeString(dataOutput, countryName);
    dataOutput.writeDouble(income);
  }

  public int compareTo(Object objKeyPair) {

    int result = income.compareTo(((CompositeKeyWritable) objKeyPair).income);

    return result;
  }

  public String getCountryName() { // getter
    return countryName;
  }

  public void setCountryName(String countryName) { // setter
    this.countryName = countryName;
  }

  public Double getIncome() { // getter
    return income;
  }

  public void setIncome(Double income) { // setter
    this.income = income;
  }
}