package com.crunch.tutor.examples.musicstore;

import java.io.IOException;

import org.apache.crunch.MapFn;

import au.com.bytecode.opencsv.CSVParser;

import com.crunch.tutor.api.music.store.Song;

/**
 * A {@link MapFn} implementation that takes in a String as input and return a {@link Song}
 * instance. Each line of the input string should be formatted of the form
 * {@code "songId","song title","artist"}
 */
public class SongMapFn extends MapFn<String, Song> {

    // Generate this since MapFn implements serializable
    private static final long serialVersionUID = -1034942975963730842L;

    // Let's create a default parser that splits based on comma(,)
    private static CSVParser CSV_PARSER = new CSVParser();

    @Override
    public Song map(final String input) {

        final Song.Builder songBuilder = Song.newBuilder();
        try {

            // For the sake of simplicity, let us assume that input file is properly formatted csv.
            final String[] parsedInputs = CSV_PARSER.parseLine(input);

            // Again, for simplicity, let us assume the input data contains the right fields and we
            // do not fail with exception. But in a real use case, we have to check for incorrectly
            // formatted data, and handle exceptions appropriately.
            songBuilder.setSongId(parsedInputs[0]);
            songBuilder.setSongTitle(parsedInputs[1]);
            songBuilder.setArtistName(parsedInputs[2]);
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return songBuilder.build();
    }

}
