package com.crunch.tutor.examples.musicstore;

import java.io.IOException;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;

import au.com.bytecode.opencsv.CSVParser;

import com.crunch.tutor.api.music.store.Song;

/**
 * A {@link DoFn} implementation that takes in a String as input and return a {@link Song} instance.
 * Each line of the input string should be formatted of the form
 * {@code "songId","song title","artist"} (or) {@code "songId","song title","artist ft. artist"} If
 * the artist includes a featuring artist, then two instances of {@link Song} will be produced.
 */
public class SongExtractorDoFn extends DoFn<String, Song> {

    // Generate this since MapFn implements serializable
    private static final long serialVersionUID = -1034942975963730842L;

    // Let's create a default parser that splits based on comma(,)
    private static CSVParser CSV_PARSER = new CSVParser();

    @Override
    public void process(final String input, final Emitter<Song> emitter) {

        String[] parsedInputs;
        try {
            // For the sake of simplicity, let us assume that input file is properly formatted csv.
            parsedInputs = CSV_PARSER.parseLine(input);
        } catch (final IOException e) {
            e.printStackTrace();
            return;
        }

        // Again, for simplicity, let us assume the input data contains the right fields and we
        // do not fail with exception. But in a real use case, we have to check for incorrectly
        // formatted data, and handle exceptions appropriately.
        final Song.Builder songBuilder = Song.newBuilder();
        songBuilder.setSongId(parsedInputs[0]);
        songBuilder.setSongTitle(parsedInputs[1]);

        // Find artist and featuring artist information
        final String[] artistNames = parsedInputs[2].split("ft. ");
        // Emit song object(s) as an output of this processing
        for (final String artistName : artistNames) {
            songBuilder.setArtistName(artistName);
            // The emitter send the output from a DoFn to the Hadoop API. It can be called any
            // number of times to produce the desired number of outputs
            emitter.emit(songBuilder.build());
        }
    }
}
