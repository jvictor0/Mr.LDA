package cc.mrlda;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.lang.text.StrBuilder;

import cc.mrlda.ParseCorpus.MyCounter;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.io.pair.PairOfInts;

public class DisplayDocument extends Configured implements Tool {
    @SuppressWarnings("unchecked")
    public int run(String[] args) throws Exception 
    {


	Job job = new Job(getConf(), "DisplayDocument");
	job.setJarByClass(Document.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	
	job.setMapperClass(Map.class);
	job.setNumReduceTasks(0); // no reducers cause I'm cool like dat
	
	job.setInputFormatClass(SequenceFileInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	

	Options options = new Options();
	
	options.addOption(Settings.HELP_OPTION, false, "print the help message");
	options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR).hasArg()
			  .withDescription("input document directory").create(Settings.INPUT_OPTION));
	options.addOption(OptionBuilder.withArgName(Settings.PATH_INDICATOR).hasArg()
			  .withDescription("output document directory").create(Settings.OUTPUT_OPTION));
	
	
	String gammaString = null;
	String outString = null;
	   
	CommandLineParser parser = new GnuParser();
	HelpFormatter formatter = new HelpFormatter();
	try {
	    CommandLine line = parser.parse(options, args);
	    
	    if (line.hasOption(Settings.HELP_OPTION)) {
		formatter.printHelp(DisplayDocument.class.getName(), options);
		System.exit(0);
	    }
	    
	    if (line.hasOption(Settings.INPUT_OPTION) && line.hasOption(Settings.OUTPUT_OPTION) )
	    {
		gammaString = line.getOptionValue(Settings.INPUT_OPTION);
		outString = line.getOptionValue(Settings.OUTPUT_OPTION);
	    } 
	    else 
	    {
		throw new ParseException("Parsing failed due to " + Settings.INPUT_OPTION
					 + " not initialized...");
	    }
	} catch (ParseException pe) {
	    System.err.println(pe.getMessage());
	    formatter.printHelp(DisplayDocument.class.getName(), options);
	    System.exit(0);
	} catch (NumberFormatException nfe) {
	    System.err.println(nfe.getMessage());
	    System.exit(0);
	}
	
	Path gammaPath = new Path(gammaString);
	Path outPath = new Path(outString);
	
	FileInputFormat.addInputPath(job, gammaPath);
	FileOutputFormat.setOutputPath(job, outPath);
	
	job.waitForCompletion(true);
		
	return 0;
    }

  public static class Map extends Mapper<IntWritable, Document, IntWritable, Text> 
  {
      private Text textOut = new Text();

      @Override
      public void map(IntWritable key, Document value, Context context) 
	  throws IOException, InterruptedException 
      {
	  StrBuilder builder = new StrBuilder();
	  builder.append(value.getTitle());
	  for (int i = 0; i < value.getNumberOfTopics(); i++) 
	  {
	      builder.append(' ');
	      builder.append(value.getGamma()[i]);
	  }
	  textOut.set(builder.toString());
	  context.write(null, textOut);
      }
  }
  
  public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new DisplayDocument(), args);
      System.exit(res);
  }
}