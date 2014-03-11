package com.analyticsmediagroup;
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.*;

// TODO: 
// 1 convert string output from weird pointer value to actual text
// 2 find out how to print out the counter values

public class FourthWallMP extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, String> {

      public enum fields {
    		id, media_market_id, station_id, date_id, household_key, mso_key, device_key, 
    		tuning_date, tuning_time, record_type, channel_number, station_name, 
    		channel_key, start_time, end_time, duration_seconds, processed
      }
    
      static enum Counters { NUM_RECORDS, WRONG_LENGTH, MISSING_DEVICE_KEY, MISSING_STATION_ID ,
    	  					 FLIPPING_CHANNEL, VALID_RECORDS, INVALID_NUMBER 
    	  					}
      
      private long numRecords = 0;
      private String inputFile;
      private static final Log LOG = LogFactory.getLog(FourthWallMP.class);

      public void configure(JobConf job) {
        inputFile = job.get("map.input.file");
      }

      public void map(LongWritable key, Text value, OutputCollector<LongWritable, String> output, Reporter reporter) throws IOException {
        
    	  String line = value.toString();
    	  String[] array = line.split("\\|");
    	  
    	  reporter.incrCounter(Counters.NUM_RECORDS, 1);
    	  if (array.length < fields.values().length) {
    		  reporter.incrCounter(Counters.WRONG_LENGTH, 1);
    		  return;
    	  }
	      if (array[fields.device_key.ordinal()] == "null") {
	    	  reporter.incrCounter(Counters.MISSING_DEVICE_KEY, 1);
	      		return;
	      }
	      if (array[fields.station_id.ordinal()] == "null") {
	    	  reporter.incrCounter(Counters.MISSING_STATION_ID, 1);
	      		return;
	      }

	      //	      if (array[fields.processed.ordinal()] == "f") {
//	      	  reporter.incrCounter(Counters.ALREADY_PROCESSED, 1);
//	    	  return;
//	      }
//	      @SuppressWarnings("deprecation")
//	      Date starttime = new Date(array[fields.start_time.ordinal()]);
//	      Date now = new Date();
//	      long diffTime = (now.getTime() - starttime.getTime()) / (1000 * 3600 * 24);
//	      if (diffTime >= 20) { // if entry start_time is more than 20 days old, ignore
//	      	  reporter.incrCounter(Counters.OLD_ENTRY, 1);
//	    	  return;
//	      }

	      // parse the duration_time into quarter hour segments
	      try {
		      if (Long.parseLong(array[fields.duration_seconds.ordinal()]) < 120) { //if duration < 2mins, ignore
		    	  reporter.incrCounter(Counters.FLIPPING_CHANNEL, 1);
		    	  return;
		      }
		      long duration = Long.parseLong(array[fields.id.ordinal()]);
	
		      for (int i=0; i < duration/900; i++) {
		       // parse into 900s or 15min segments
		    	  array[fields.duration_seconds.ordinal()] = "900";
		    	  reporter.incrCounter(Counters.VALID_RECORDS, 1);
			      StringBuilder builder = new StringBuilder();
			      for (String s: array) {
			    	  builder.append(s);
			      }
		    	  output.collect(new LongWritable(Long.parseLong(array[fields.id.ordinal()])), builder.toString());
		      }
		      	// leftover time is packaged into another segment
		    	  array[fields.duration_seconds.ordinal()] = Long.toString(duration%900);
		    	  reporter.incrCounter(Counters.VALID_RECORDS, 1);
			      StringBuilder builder = new StringBuilder();
			      for (String s: array) {
			    	  builder.append(s);
			      }

		    	  output.collect(new LongWritable(Long.parseLong(array[fields.id.ordinal()])), builder.toString());

		      if ((++numRecords % 100) == 0) {
		    	  reporter.setStatus("Finished processing " + numRecords + " records " + "from the input file: " + inputFile);
		      }
	      } catch(NumberFormatException e) {
	    	  reporter.incrCounter(Counters.INVALID_NUMBER, 1);
	    	  LOG.error("duration_seconds has invalid value: " + array[fields.duration_seconds.ordinal()]);
	          System.err.println("Caught exception while parsing the cached file '" +  "' : " + StringUtils.stringifyException(e));
	    	  return;
	      }

      }
    }

    public int run(String[] args) throws Exception {
      JobConf conf = new JobConf(getConf(), FourthWallMP.class);
      conf.setJobName("fourthwall splice");
      conf.setJarByClass(FourthWallMP.class);

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(String.class);

      conf.setMapperClass(Map.class);
      conf.setNumReduceTasks(0);
      
      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
	    
      FileInputFormat.setInputPaths(conf, new Path(args[0]));
      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
      
      JobClient.runJob(conf);
      return 0;
    }

    public static void main(String[] args) throws Exception {
    	int res = ToolRunner.run(new Configuration(), new FourthWallMP(), args);
    	System.exit(res);
    }
}
