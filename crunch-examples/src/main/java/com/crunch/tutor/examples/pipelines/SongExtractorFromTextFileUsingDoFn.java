package com.crunch.tutor.examples.pipelines;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.avro.AvroFileTarget;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.crunch.tutor.api.music.store.Song;
import com.crunch.tutor.examples.musicstore.SongExtractorDoFn;

public class SongExtractorFromTextFileUsingDoFn extends Configured implements Tool {

    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run(new Configuration(),
                new SongExtractorFromTextFileUsingDoFn(), args);
        System.exit(res);
    }

    @Override
    public int run(final String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: hadoop jar <jar-file> input-path output-path");
            System.exit(1);
        }
        final Configuration config = getConf();
        final Pipeline songExtractorPipeline = new MRPipeline(
                SongExtractorFromTextFileUsingDoFn.class, "SongExtractorFromTextFileUsingDoFn",
                config);

        // Read text file from HDFS
        final PCollection<String> songsAsStrings = songExtractorPipeline.readTextFile(args[0]);
        // Parse the input from text file and create Song objects. We use Avros#Records to determine
        // the PType of the resulting collection. A PType is a Crunch way of representing the type
        // of a PCollection that will instruct the framework on how to read/write data from the
        // PCollection.
        final PCollection<Song> songsFromStrings = songsAsStrings.parallelDo(
                "Build Song Objects from text file", new SongExtractorDoFn(),
                Avros.records(Song.class));
        // Write all song objects to HDFS
        songExtractorPipeline.write(songsFromStrings, new AvroFileTarget(args[1]));

        // Execute the pipeline by calling Pipeline#done()
        final PipelineResult result = songExtractorPipeline.done();
        return result.succeeded() ? 0 : 1;
    }
}
