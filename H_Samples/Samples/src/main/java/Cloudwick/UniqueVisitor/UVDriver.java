package Cloudwick.UniqueVisitor;

/*
 * Unique visitor count: logs (ipaddress, website).
 * Input - usera - google; usera - google ; userb - wiki ; usera - wiki
 * Output - google  1; wiki   2
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UVDriver {

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    Path outputDir1 = new Path(args[2]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    Job job = new Job(conf, "UVJob");
    job.setJarByClass(UVDriver.class);

    Job job1 = new Job(conf, "UVJob");
    job1.setJarByClass(UVDriver.class);

    // Setup MapReduce
    job.setMapperClass(UVmapper1.class);
    job1.setMapperClass(UVmapper2.class);
    job.setReducerClass(UVreducer1.class);
    job1.setReducerClass(UVreducer2.class);

    job.setNumReduceTasks(1);
    job1.setNumReduceTasks(1);

    // Specify key / value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    // Input
    FileInputFormat.addInputPath(job, inputPath);
    job.setInputFormatClass(TextInputFormat.class);

    FileInputFormat.addInputPath(job1, outputDir);
    job1.setInputFormatClass(TextInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileOutputFormat.setOutputPath(job1, outputDir1);
    job1.setOutputFormatClass(TextOutputFormat.class);

    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(outputDir))
      hdfs.delete(outputDir, true);

    // Execute job
    int code = job.waitForCompletion(true) ? 0 : 1;

    // if first job fails, just exit immediately with error code 1;
    if (code != 0)
      System.exit(code);

    code = job1.waitForCompletion(true) ? 0 : 2;
    System.exit(code);
  }
}
