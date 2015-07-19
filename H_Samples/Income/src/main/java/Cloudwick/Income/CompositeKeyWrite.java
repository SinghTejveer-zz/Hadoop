package Cloudwick.Income;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

@SuppressWarnings("rawtypes")
public class CompositeKeyWrite implements WritableComparable {

  private String countryName;
  private Double income;

  public CompositeKeyWrite() {
  }

  public CompositeKeyWrite(String countryName, Double income) {
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

  @Override
  public boolean equals(Object other) {

    // check for reference equality
    if (this == other)
      return true;

    if (!(other instanceof CompositeKeyWrite))
      return false;

    // cast to native object is now safe
    CompositeKeyWrite that = (CompositeKeyWrite) other;

    // now a proper field-by-field evaluation can be made
    return this.countryName.equals(that.countryName)
        && this.income.equals(that.income);
  }

  public int compareTo(Object objKeyPair) {

    int result = income.compareTo(((CompositeKeyWrite) objKeyPair).income);

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