package com.crunch.tutor.examples.pipelines;

import org.apache.crunch.FilterFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.crunch.tutor.api.music.store.Song;

public class PipelineWithFilterFn extends Configured implements Tool {

    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run(new Configuration(), new PipelineWithFilterFn(), args);
        System.exit(res);
    }

    @Override
    public int run(final String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: hadoop jar <jar-file> input-path output-path");
            System.exit(1);
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        final Configuration config = getConf();
        final Pipeline pipeline = new MRPipeline(PipelineWithFilterFn.class,
                "PipelineWithFilterFn", config);
        // read Song object that are persisted in HDFS as avro
        final PCollection<Song> songsCollection = pipeline.read(From.avroFile(inputPath,
                Avros.records(Song.class)));
        // filter out songs that has a null genre
        final PCollection<Song> songsWithValidGenre = songsCollection.filter(
                "Filter songs with invalid genre's", new SongsWithGenreFilterFn());
        // write the filtered song objects to HDFS
        pipeline.write(songsWithValidGenre, To.avroFile(outputPath));

        // Execute the pipeline by calling Pipeline#done()
        final PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }

    /**
     * A {@link FilterFn} implementation that filters {@link Song} records that have a non-null
     * {@link Song#getGenre()}
     */
    private static class SongsWithGenreFilterFn extends FilterFn<Song> {

        private static final long serialVersionUID = 1673909907356321796L;

        @Override
        public boolean accept(final Song input) {

            // if the song has a genre, include it
            if (input.getGenre() != null) {
                return true;
            } else {
                return false;
            }
        }
    }
}
