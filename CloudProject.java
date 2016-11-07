package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import javafx.util.converter.NumberStringConverter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class CloudProject extends Configured implements Tool {
	public static float democratsWin = 0, republicansWin = 0, numLines = 0, totAvgAcc = 0.0f;

	public CloudProject() {
	}

	private static final Logger LOG = Logger.getLogger(CloudProject.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CloudProject(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		String fileName = "Input.csv";
		String line = null;

		try {
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				// counting the total number of lines for the calculation of
				// total number of pollster results
				numLines++;

				String[] lineParts = line.split(",");

				// calculating average of accuracy
				totAvgAcc += Float.parseFloat(lineParts[2]);

				// calulating the number of Democrats and Republicans that have
				// won over the years
				if (lineParts[3].equals("Democratic"))
					democratsWin++;
				else
					republicansWin++;
			}
			bufferedReader.close();
		} catch (FileNotFoundException e) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");
		}

		// calculating the average accuracy for getting the candidate that is to
		// be classified
		totAvgAcc /= numLines;

		// sending the values calculated in the outer class to the reducer
		// function
		Configuration configuration = new Configuration();
		configuration.set("numberOfLines", numLines + "");
		configuration.set("numberDemocrats", democratsWin + "");
		configuration.set("numberRepublicans", republicansWin + "");
		configuration.set("totAcc", totAvgAcc + "");
		Job job = Job.getInstance(configuration, " CloudProject ");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	// First mapper
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			Text currentWord = new Text();
			String[] parts = line.split(",");

			// mapping the name of the state with the grade, pollster accuracy &
			// past popularity
			context.write(new Text(parts[0]), new Text(parts[1] + "," + parts[2] + "," + parts[3]));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {

			// accepting the calculated values from the outer class
			Configuration conf = context.getConfiguration();
			String temp = conf.get("numberOfLines");
			String temp1 = conf.get("numberDemocrats");
			String temp2 = conf.get("numberRepublicans");
			String temp3 = conf.get("totAcc");
			float numberOfLines = Float.parseFloat(temp);
			float numberDemocrats = Float.parseFloat(temp1);
			float numberRepublicans = Float.parseFloat(temp2);
			float totAcc = Float.parseFloat(temp3);

			// initializing all the variables
			float grade = 0.0f, accuracy = 0.0f, numGradeDemocrats = 0.0f, numGradeRepublicans = 0.0f,
					numAccDemocrats = 0.0f, numAccRepublicans = 0.0f, numStateLines = 0.0f, numStateDemocrats = 0.0f,
					numStateRepublicans = 0.0f;

			// array list to store the values to use them more than once
			ArrayList<Text> tempArrList = new ArrayList<Text>();

			for (Text count : counts) {
				tempArrList.add(count);

				// to find the total number of pollster in a particular state
				numStateLines++;
				String line = count.toString();
				String[] parts = line.split(",");

				// counting the number of democrats & republicans in each state
				if (parts[2].equals("Democratic"))
					numStateDemocrats++;
				else if (parts[2].equals("Republican"))
					numStateRepublicans++;

				grade += Float.parseFloat(parts[0]);
				accuracy += Float.parseFloat(parts[1]);
			}

			// calculating the average grade & average accuracy for getting a
			// classifiable instance
			float avgGrade = grade / numStateLines, avgAccuracy = accuracy / numStateLines, newAvgAccuracy = 0.0f;
			// the grade gets rounded off to the nearest integer
			avgGrade = Math.round(avgGrade);

			// categorizing the accuracy by comparing it with the average
			// accuracy
			if (avgAccuracy > totAcc)
				newAvgAccuracy = 1.0f;
			else
				newAvgAccuracy = 0.0f;

			for (Text count : tempArrList) {
				String line = count.toString();
				String[] parts = line.split(",");

				// finding the number of democrats and republicans with
				// grade matching the that of the classifiable
				// instance
				if (Float.parseFloat(parts[0]) == avgGrade && parts[2].equals("Democratic"))
					numGradeDemocrats++;
				else if (Float.parseFloat(parts[0]) == avgGrade && parts[2].equals("Republican"))
					numGradeRepublicans++;

				// finding the number of democrats and republicans with
				// accuracy matching the that of the classifiable
				// instance
				if (newAvgAccuracy == Float.parseFloat(parts[1]) && parts[2].equals("Democratic"))
					numAccDemocrats++;
				else if (newAvgAccuracy == Float.parseFloat(parts[1]) && parts[2].equals("Republican"))
					numAccRepublicans++;
			}

			float probDemo = 0.0f, probRepub = 0.0f;
			float alpha = 1.0f, States = 34.0f;

			// implementing naive bayes to classify the instance as a democrat
			// or a republican
			probDemo = (numberDemocrats / numberOfLines)
					* ((numStateDemocrats + alpha) / (numberDemocrats + alpha * States))
					* ((numGradeDemocrats + alpha) / (numberDemocrats + alpha * States))
					* ((numAccDemocrats + alpha) / (numberDemocrats + alpha * States));

			probRepub = (numberRepublicans / numberOfLines)
					* ((numStateRepublicans + alpha) / (numberRepublicans + alpha * States))
					* ((numGradeRepublicans + alpha) / (numberRepublicans + alpha * States))
					* ((numAccRepublicans + alpha) / (numberRepublicans + alpha * States));

			// actual classification
			if (probDemo > probRepub)
				context.write(word, new Text("Democratic"));
			else if (probDemo < probRepub)
				context.write(word, new Text("Republican"));

		}
	}
}
