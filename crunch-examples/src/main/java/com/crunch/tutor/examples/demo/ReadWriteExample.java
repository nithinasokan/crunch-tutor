package com.crunch.tutor.examples.demo;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.hadoop.conf.Configuration;

/**
 * A simple program using Apache Crunch API that reads and writes text files to HDFS
 * 
 */
public class ReadWriteExample {

    public static void main(final String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: hadoop jar <jar-file> input-path output-path");
            System.exit(1);
        }

        // Get input and output paths
        final String inputPath = args[0];
        final String outputPath = args[1];

        // Create an instance of Pipeline by providing ClassName and the job configuration.
        final Pipeline pipeline = new MRPipeline(ReadWriteExample.class, new Configuration());

        // Read a text file
        final PCollection<String> inputLines = pipeline.readTextFile(inputPath);

        // write the text file
        pipeline.writeTextFile(inputLines, outputPath);

        // Execute the pipeline by calling Pipeline#done()
        final PipelineResult result = pipeline.done();

        System.exit(result.succeeded() ? 0 : 1);
    }
}
