package Cloudwick.Income;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import Cloudwick.Income.CompositeKeyWrite;
import Cloudwick.Income.CountryIncomeMapper;
import Cloudwick.Income.CountryIncomeReducer;

public class CountryIncomeReducerTest {

  MapDriver<Object, Text, CompositeKeyWrite, NullWritable> mapDriver;
  ReduceDriver<CompositeKeyWrite, NullWritable, CompositeKeyWrite, NullWritable> reduceDriver;
  MapReduceDriver<Object, Text, CompositeKeyWrite, NullWritable, CompositeKeyWrite, NullWritable> mapReduceDriver;

  @Before
  public void setUp() {
    CountryIncomeMapper mapper = new CountryIncomeMapper();
    CountryIncomeReducer reducer = new CountryIncomeReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testReducer_ValidLine_ValidMap() throws IOException {

    // Arrange
    CompositeKeyWrite k = new CompositeKeyWrite("Norway", 65636.7724056934);
    List<NullWritable> values = new ArrayList<NullWritable>();
    values.add(NullWritable.get());

    reduceDriver.withInput(k, values);
    reduceDriver.withOutput(k, NullWritable.get());

    // Act and Assert
    reduceDriver.runTest();
  }

}
