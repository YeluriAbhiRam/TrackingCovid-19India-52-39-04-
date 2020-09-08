package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.StringReader;
import com.opencsv.CSVReader;

public class CovidStats extends Configured implements Tool {
private static long start,end;

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new CovidStats(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
		start = new Date().getTime();
    	Job job1 = Job.getInstance(getConf(), "CountTotalCases");
		Job job2 = Job.getInstance(getConf(), "sortByConfirmed");
		Job job3 = Job.getInstance(getConf(), "sortByRecovered");
		Job job4 = Job.getInstance(getConf(), "sortByDeaths");
		Job job5 = Job.getInstance(getConf(), "genderStats");
		Job job8 = Job.getInstance(getConf(), "ageStats");
		Job job9 = Job.getInstance(getConf(), "nationStats");
		Job job6 = Job.getInstance(getConf(), "TotalStats");
		Job job7 = Job.getInstance(getConf(), "sortByActive");

		job1.setJarByClass(CovidStats.class);
		job1.setMapperClass(MapProc.class);
		job1.setReducerClass(ReduceProc.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]+"newdatatset.csv"));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]+"main"));
		job1.waitForCompletion(true);
		end = new Date().getTime();
		System.out.println("\nJob 1 took " + (end-start) + "milliseconds\n");

		job2.setJarByClass(CovidStats.class);
		job2.setMapperClass(MapConf.class);
		job2.setReducerClass(ReduceSort.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]+"main"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"ConfirmedSort"));
		job2.waitForCompletion(true);

		end = new Date().getTime();
		System.out.println("\nJob 2 took " + (end-start) + "milliseconds\n");

		job3.setJarByClass(CovidStats.class);
		job3.setMapperClass(MapRec.class);
		job3.setReducerClass(ReduceSort.class);
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, new Path(args[1]+"main"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"RecoveredSort"));
		job3.waitForCompletion(true);

		end = new Date().getTime();
		System.out.println("\nJob 3 took " + (end-start) + "milliseconds\n");

		job4.setJarByClass(CovidStats.class);
		job4.setMapperClass(MapDeath.class);
		job4.setReducerClass(ReduceSort.class);
		job4.setMapOutputKeyClass(LongWritable.class);
		job4.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4, new Path(args[1]+"main"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]+"DeathSort"));
		job4.waitForCompletion(true);

		end = new Date().getTime();
		System.out.println("\nJob 4 took " + (end-start) + "milliseconds\n");
		
		job5.setJarByClass(CovidStats.class);
		job5.setMapperClass(MapGender.class);
		job5.setReducerClass(ReduceCounts.class);
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job5, new Path(args[0]+"genDataset.csv"));
		FileOutputFormat.setOutputPath(job5, new Path(args[1]+"GenderStats"));
		job5.waitForCompletion(true);

		end = new Date().getTime();
		System.out.println("\nJob 5 took " + (end-start) + "milliseconds\n");
		
		job8.setJarByClass(CovidStats.class);
		job8.setMapperClass(MapAge.class);
		job8.setReducerClass(ReduceCounts.class);
		job8.setMapOutputKeyClass(Text.class);
		job8.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job8, new Path(args[0]+"genDataset.csv"));
		FileOutputFormat.setOutputPath(job8, new Path(args[1]+"AgeStats"));
		job8.waitForCompletion(true);

		end = new Date().getTime();
		System.out.println("\nJob 8 took " + (end-start) + "milliseconds\n");
		
		job9.setJarByClass(CovidStats.class);
		job9.setMapperClass(MapNation.class);
		job9.setReducerClass(ReduceCounts.class);
		job9.setMapOutputKeyClass(Text.class);
		job9.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job9, new Path(args[0]+"genDataset.csv"));
		FileOutputFormat.setOutputPath(job9, new Path(args[1]+"NationStats"));
		job9.waitForCompletion(true);
		
		end = new Date().getTime();
		System.out.println("\nJob 9 took " + (end-start) + "milliseconds\n");
		
		
		job6.setJarByClass(CovidStats.class);
		job6.setMapperClass(MapStats.class);
		job6.setReducerClass(ReduceStats.class);
		job6.setMapOutputKeyClass(Text.class);
		job6.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job6, new Path(args[1]+"main"));
		FileOutputFormat.setOutputPath(job6, new Path(args[1]+"OverallStats"));
		job6.waitForCompletion(true);

		end = new Date().getTime();
		System.out.println("\nJob 6 took " + (end-start) + "milliseconds\n");

		job7.setJarByClass(CovidStats.class);
		job7.setMapperClass(MapActive.class);
		job7.setReducerClass(ReduceSort.class);
		job7.setMapOutputKeyClass(LongWritable.class);
		job7.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job7, new Path(args[1]+"main"));
		FileOutputFormat.setOutputPath(job7, new Path(args[1]+"ActiveSort"));


		boolean status = job7.waitForCompletion(true);
		if (status == true) 
		{
			end = new Date().getTime();
			System.out.println("\nJob 7 took " + (end-start) + "milliseconds\n");
			return 0;
		}
		else
			return 1;
  }
  
  public static class MapProc extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private long numRecords = 0;    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text(),currentVal;
		if(line.length()>0)
		{
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] nextLine;
			try {
	    		  	if ((nextLine = reader.readNext()) != null) 
				{
		        	 if(nextLine.length==7)
		        	 {	
					String stName= nextLine[1].trim();
					currentWord = new Text(stName);
					Long Confcount=Long.parseLong(nextLine[3].trim());
					Long Reccount=Long.parseLong(nextLine[4].trim());
					Long Deathcount=Long.parseLong(nextLine[5].trim());
					Long Activecount=Long.parseLong(nextLine[6].trim());
					currentVal=new Text(stName+"\t"+Confcount+"\t"+Reccount+"\t"+Deathcount+"\t"+Activecount);
		        		context.write(currentWord,currentVal);
				 }
		        	 else
		        	 { throw new Exception();}

	    		  	}
	    		  }
	    	  	catch(NoClassDefFoundError e)
	    	  	{}
	    	  	catch(Exception e)
	    	  	{}
	    	  	finally {reader.close();}
		}
        }
    }
	
	public static class MapConf extends Mapper<LongWritable, Text, LongWritable, Text> {
    private Text word = new Text();
    private long numRecords = 0;    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text(),currentVal;
	  LongWritable currentKey = new LongWritable();
		if(line.length()>0)
		{
			CSVReader reader = new CSVReader(new StringReader(line), '\t' , '"' ,0);
			String[] nextLine;
			try {
	    		  	if ((nextLine = reader.readNext()) != null) 
				{
		        	 if(nextLine.length==5)
		        	 {	
					String stName= nextLine[0].trim();
					String ConfTot=nextLine[1].trim();
					String RecTot=nextLine[2].trim();
					String DeathTot=nextLine[3].trim();
					String ActiveTot=nextLine[4].trim();
					currentKey=new LongWritable(Long.parseLong(ConfTot));
					currentVal=new Text(stName+"\t"+RecTot+"\t"+DeathTot+"\t"+ActiveTot);
		        		context.write(currentKey,currentVal);
				 }
		        	 else
		        	 { throw new Exception();}

	    		  	}
	    		  }
	    	  	catch(NoClassDefFoundError e)
	    	  	{}
	    	  	catch(Exception e)
	    	  	{}
	    	  	finally {reader.close();}
		}
        }
    }
	public static class MapRec extends Mapper<LongWritable, Text, LongWritable, Text> {
    private Text word = new Text();
    private long numRecords = 0;    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text(),currentVal;
	  LongWritable currentKey = new LongWritable();
		if(line.length()>0)
		{
			CSVReader reader = new CSVReader(new StringReader(line), '\t' , '"' ,0);
			String[] nextLine;
			try {
	    		  	if ((nextLine = reader.readNext()) != null) 
				{
		        	 if(nextLine.length==5)
		        	 {	
					String stName= nextLine[0].trim();
					String ConfTot=nextLine[1].trim();
					String RecTot=nextLine[2].trim();
					String DeathTot=nextLine[3].trim();
					String ActiveTot=nextLine[4].trim();
					currentKey=new LongWritable(Long.parseLong(RecTot));
					currentVal=new Text(stName+"\t"+ConfTot+"\t"+DeathTot+"\t"+ActiveTot);
		        		context.write(currentKey,currentVal);
				 }
		        	 else
		        	 { throw new Exception();}

	    		  	}
	    		  }
	    	  	catch(NoClassDefFoundError e)
	    	  	{}
	    	  	catch(Exception e)
	    	  	{}
	    	  	finally {reader.close();}
		}
        }
    }
	public static class MapDeath extends Mapper<LongWritable, Text, LongWritable, Text> {
    private Text word = new Text();
    private long numRecords = 0;    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text(),currentVal;
	  LongWritable currentKey = new LongWritable();
		if(line.length()>0)
		{
			CSVReader reader = new CSVReader(new StringReader(line), '\t' , '"' ,0);
			String[] nextLine;
			try {
	    		  	if ((nextLine = reader.readNext()) != null) 
				{
		        	 if(nextLine.length==5)
		        	 {	
					String stName= nextLine[0].trim();
					String ConfTot=nextLine[1].trim();
					String RecTot=nextLine[2].trim();
					String DeathTot=nextLine[3].trim();
					String ActiveTot=nextLine[4].trim();
					currentKey=new LongWritable(Long.parseLong(DeathTot));
					currentVal=new Text(stName+"\t"+ConfTot+"\t"+RecTot+"\t"+ActiveTot);
		        		context.write(currentKey,currentVal);
				 }
		        	 else
		        	 { throw new Exception();}

	    		  	}
	    		  }
	    	  	catch(NoClassDefFoundError e)
	    	  	{}
	    	  	catch(Exception e)
	    	  	{}
	    	  	finally {reader.close();}
		}
        }
	}
    
	public static class MapActive extends Mapper<LongWritable, Text, LongWritable, Text> {
    private Text word = new Text();
    private long numRecords = 0;    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text(),currentVal;
	  LongWritable currentKey = new LongWritable();
		if(line.length()>0)
		{
			CSVReader reader = new CSVReader(new StringReader(line), '\t' , '"' ,0);
			String[] nextLine;
			try {
	    		  	if ((nextLine = reader.readNext()) != null) 
				{
		        	 if(nextLine.length==5)
		        	 {	
					String stName= nextLine[0].trim();
					String ConfTot=nextLine[1].trim();
					String RecTot=nextLine[2].trim();
					String DeathTot=nextLine[3].trim();
					String ActiveTot=nextLine[4].trim();
					currentKey=new LongWritable(Long.parseLong(ActiveTot));
					currentVal=new Text(stName+"\t"+ConfTot+"\t"+RecTot+"\t"+DeathTot);
		        		context.write(currentKey,currentVal);
				 }
		        	 else
		        	 { throw new Exception();}

	    		  	}
	    		  }
	    	  	catch(NoClassDefFoundError e)
	    	  	{}
	    	  	catch(Exception e)
	    	  	{}
	    	  	finally {reader.close();}
		}
        }
    }
	public static class MapStats extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private long numRecords = 0;    
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text();
	  Text currentVal=new Text();
	  LongWritable currentKey = new LongWritable();
		if(line.length()>0)
		{
			CSVReader reader = new CSVReader(new StringReader(line), '\t' , '"' ,0);
			String[] nextLine;
			try {
	    		  	if ((nextLine = reader.readNext()) != null) 
				{
		        	 if(nextLine.length==5)
		        	 {	
					String stName= nextLine[0].trim();
					String ConfTot=nextLine[1].trim();
					String RecTot=nextLine[2].trim();
					String DeathTot=nextLine[3].trim();
					String ActiveTot=nextLine[4].trim();
					currentWord=new Text("Total Confirmed Cases"+"\t"+"Total Recovered Cases"+"\t"+"Total Deaths"+"\t"+"Total Active Cases"+"\n");
					currentVal=new Text(ConfTot+"\t"+RecTot+"\t"+DeathTot+"\t"+ActiveTot);
		        		context.write(currentWord,currentVal);
				 }
		        	 else
		        	 { throw new Exception();}

	    		  	}
	    		  }
	    	  	catch(NoClassDefFoundError e)
	    	  	{}
	    	  	catch(Exception e)
	    	  	{}
	    	  	finally {reader.close();}
		}
        }
    }
	public static class MapGender extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text();
		if(line.length()>0)
		{
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] nextLine;
			try {
	    		  	if ((nextLine = reader.readNext()) != null) 
				{
		        	 if(nextLine.length==20)
		        	 {	
					String gender= nextLine[5].trim();
					currentWord = new Text(gender);
		        		context.write(currentWord,one);
				 }
		        	 else
		        	 { throw new Exception();}

	    		  	}
	    		  }
	    	  	catch(NoClassDefFoundError e)
	    	  	{}
	    	  	catch(Exception e)
	    	  	{}
		}
        }
    }

public static class MapAge extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text();
		if(line.length()>0)
		{
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] nextLine;
			try {
	    		  	if ((nextLine = reader.readNext()) != null) 
				{
		        	 if(nextLine.length==20)
		        	 {	
					String age= nextLine[4].trim();
					currentWord = new Text(age);
		        		context.write(currentWord,one);
				 }
		        	 else
		        	 { throw new Exception();}

	    		  	}
	    		  }
	    	  	catch(NoClassDefFoundError e)
	    	  	{}
	    	  	catch(Exception e)
	    	  	{}
		}
        }
    }

	public static class MapNation extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text();
		if(line.length()>0)
		{
			CSVReader reader = new CSVReader(new StringReader(line));
			String[] nextLine;
			try {
	    		  	if ((nextLine = reader.readNext()) != null) 
				{
		        	 if(nextLine.length==20)
		        	 {	
					String nation= nextLine[13].trim();
					currentWord = new Text(nation);
		        		context.write(currentWord,one);
				 }
		        	 else
		        	 { throw new Exception();}

	    		  	}
	    		  }
	    	  	catch(NoClassDefFoundError e)
	    	  	{}
	    	  	catch(Exception e)
	    	  	{}
		}
        }
    }
	
	public static class ReduceProc extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text word, Iterable<Text> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0,sum1=0,sum2=0,sum3=0;
	  int total=0,total1=0,total2=0,total3=0;
	  String cVal;
	Text newVal=new Text();
      for (Text count : counts) {
		cVal=count.toString();
		CSVReader reader = new CSVReader(new StringReader(cVal), '\t');
		String[] nextLine=reader.readNext();
		total=Integer.parseInt(nextLine[1].trim());
		total1=Integer.parseInt(nextLine[2].trim());
		total2=Integer.parseInt(nextLine[3].trim());
		total3=Integer.parseInt(nextLine[4].trim());
		sum += total;
		sum1 += total1;
		sum2+= total2;
		sum3 += total3;
		newVal=new Text(sum+"\t"+sum1+"\t"+sum2+"\t"+sum3);
      }
      context.write(word,newVal);
    }
	}
	public static class ReduceSort extends Reducer<LongWritable, Text, LongWritable, Text> {
    public void reduce(LongWritable word, Iterable<Text> vals, Context context)
        throws IOException, InterruptedException {
	long max=word.get();
	word =new LongWritable(max);
      for (Text val : vals)
	context.write(word, val);
      
    }
  }
public static class ReduceStats extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text word, Iterable<Text> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0,sum1=0,sum2=0,sum3=0;
	  int total=0,total1=0,total2=0,total3=0;
	  String cVal;
	Text newVal=new Text();
      for (Text count : counts) {
		cVal=count.toString();
		CSVReader reader = new CSVReader(new StringReader(cVal), '\t');
		String[] nextLine=reader.readNext();
		total=Integer.parseInt(nextLine[0].trim());
		total1=Integer.parseInt(nextLine[1].trim());
		total2=Integer.parseInt(nextLine[2].trim());
		total3=Integer.parseInt(nextLine[3].trim());
		sum += total;
		sum1 += total1;
		sum2+= total2;
		sum3 += total3;
		newVal=new Text(sum+"\t"+sum1+"\t"+sum2+"\t"+sum3);
      }
      context.write(word,newVal);
    }
	}
   public static class ReduceCounts extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  } 
  
}
