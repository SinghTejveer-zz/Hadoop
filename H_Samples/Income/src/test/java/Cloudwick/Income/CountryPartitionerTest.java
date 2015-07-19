package Cloudwick.Income;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

import Cloudwick.Income.CompositeKeyWrite;

public class CountryPartitionerTest {

  @Test
  public void getPartition_SameCountries_SamePartition() throws IOException {

    // Arrange
    CompositeKeyWrite inp0 =
        new CompositeKeyWrite("Switzerland", 63318.3487591229);
    CompositeKeyWrite inp1 =
        new CompositeKeyWrite("Switzerland", 65636.7724056934);
    CountryPartitioner uut = new CountryPartitioner();

    // Act
    int actual0 = uut.getPartition(inp0, NullWritable.get(), 100);
    int actual1 = uut.getPartition(inp1, NullWritable.get(), 100);

    // Assert
    assertEquals(actual0, actual1);

  }

  @Test
  public void getPartition_DiffCountries_DiffPartition() throws IOException {

    // Arrange
    CompositeKeyWrite inp0 =
        new CompositeKeyWrite("Switzerland", 63318.3487591229);
    CompositeKeyWrite inp1 = new CompositeKeyWrite("Norway", 65636.7724056934);
    CountryPartitioner uut = new CountryPartitioner();

    // Act
    int actual0 = uut.getPartition(inp0, NullWritable.get(), 100);
    int actual1 = uut.getPartition(inp1, NullWritable.get(), 100);

    // Assert
    assertTrue(actual0 != actual1);

  }
}