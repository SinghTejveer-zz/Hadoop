package Cloudwick.Income;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
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

public class CountryIncomeMapperTest {

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
  public void testMapper_ValidLine_ValidMap() throws IOException {

    // Arrange

    mapDriver
        .withInput(
            new LongWritable(),
            new Text(
                "   Afghanistan   ,AFG,Adjusted net national income per capita (current US$),NY.ADJ.NNTY.PC.CD,,,,,,,,,,,1.50855789775009E+02,1.53323029993147E+02,1.29938741127974E+02,1.34638308666771E+02,1.62841258802338E+02,1.74029361459106E+02,1.84657885719044E+02,2.10013898514467E+02,2.32910721424455E+02,2.60386697008870E+02,2.49613937173977E+02,2.39907716924562E+02,,,,,,,,,,,,,,,,,,,,1.02143433887964E+02,1.63667032790391E+02,1.76387812398029E+02,1.97829227159392E+02,2.28997715275721E+02,2.48507069374298E+02,3.37218077773925E+02,3.37420503202262E+02,4.06377005590016E+02,5.00880766813369E+02,5.49832839819594E+02,6.18282906047745E+02,5.98572514404039E+02,"));
    CompositeKeyWrite k =
        new CompositeKeyWrite("Afghanistan", 500.880766813369);

    mapDriver.withOutput(k, NullWritable.get());

    // Act and Assert
    mapDriver.runTest();
  }

}
