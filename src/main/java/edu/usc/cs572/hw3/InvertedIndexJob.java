package edu.usc.wordindexer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

/*
 * @Author : Mukesh Dangi
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job indexerJob = Job.getInstance(conf, "Word Indexer Job");

        FileInputFormat.addInputPath(indexerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(indexerJob, new Path(args[1]));

        indexerJob.setOutputKeyClass(Text.class);
        indexerJob.setOutputValueClass(Text.class);

        indexerJob.setReducerClass(DocIdReducer.class);
        indexerJob.setMapperClass(TokenizeMapper.class);

        indexerJob.setJarByClass(InvertedIndexJob.class);
        boolean status = indexerJob.waitForCompletion(true);
        System.exit(status ? 0 : 1);

    }

    public void preProcess(String[] args) throws Exception {

    }

    public static void updateWordCount() {

    }

    public static class TokenizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text word  = new Text();
        private Text docid = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = value.toString().split("\\t", 2);
            docid = new Text(tokens[0].trim());
            line = tokens[1].toLowerCase().replaceAll("[0-9,'-.`/:?]", " ").trim();

            StringTokenizer tknOfLine = new StringTokenizer(line);

            while (tknOfLine.hasMoreTokens()) {
                String actToken = tknOfLine.nextToken();

                actToken = actToken.replaceAll("[^a-zA-Z]", "").trim();
                if (!actToken.isEmpty()) {
                    word.set(actToken);
                    context.write(word, docid);
                }
            }

        }
    }

    public static class DocIdReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> invertedIndxMap = new HashMap<String, Integer>();
            Iterator<Text> textItr = values.iterator();
            int wordFreq = 0;
            String word;

            while (textItr.hasNext()) {
                word = textItr.next().toString();
                if (invertedIndxMap.containsKey(word)) {
                    wordFreq = (invertedIndxMap.get(word));
                    wordFreq += 1;
                    invertedIndxMap.put(word, wordFreq);
                } else {
                    invertedIndxMap.put(word, 1);
                }

            }

            StringBuffer strBuffer = new StringBuffer("");
            for (Map.Entry<String, Integer> map : invertedIndxMap.entrySet()) {
                strBuffer.append(map.getKey() + ":" + map.getValue() + "\t");
            }
            context.write(key, new Text(strBuffer.toString()));

        }
    }

    public void process() {

    }

    public void postProcess() {

    }

}
