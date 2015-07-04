package Cloudwick.JobChain;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JobChainDriver {

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {

    Path inputPath1 = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    Path outputDir1 = new Path(args[2]);

    // Create configuration
    Configuration conf = new Configuration(true);

    // Create job
    Job job = new Job(conf, "Job_Chain");
    job.setJarByClass(JobChainDriver.class);

    Job job1 = new Job(conf, "Job_Chain");
    job1.setJarByClass(JobChainDriver.class);

    // Setup MapReduce
    job.setMapperClass(ChainMapper1.class);
    job1.setMapperClass(ChainMapper2.class);
    // job.setReducerClass(WordCountReducer.class);
    job.setNumReduceTasks(1);

    // Specify key / value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    // Input
    FileInputFormat.addInputPath(job, inputPath1);
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
    int code1 = job1.waitForCompletion(true) ? 0 : 1;
    System.exit(code1);

  }

}
