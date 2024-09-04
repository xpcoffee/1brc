/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;

/**
 * -- 6: allow parallel on readMeasurements spliterator ; got faster ver time (CPU caching I think)
 * mean 33.281
 * Range (min  max):   27.912 s  37.837 s
 * <p>
 * -- 5: try to create segments v2
 * mean 75.7 ... between (28 s and 200s)!!
 * <p>
 * -- 4: try to create segments
 * mean 240 s
 * <p>
 * -- 3: don't split(";") for arguments
 * mean 75.248 s
 * <p>
 * -- 2: don't create new aggregator
 * mean 83.730 s
 * <p>
 * -- 1: stream.parallel
 * mean 83.423
 * <p>
 * -- 0: baseline
 * mean 121.768
 */
public class CalculateAverage_xpcoffee {
    private static final String FILE = "./measurements.txt";
    // private static final String FILE =
    // "./src/test/resources/samples/measurements-1.txt";

    private static final char EOL = '\n';
    private static final char SEPARATOR = ';';

    public static void main(String[] args) throws IOException {
        var file = new RandomAccessFile(FILE, "r");
        var fileChannel = file.getChannel();
        var threads = Runtime.getRuntime().availableProcessors();
        var segments = getSegments(file, threads);

        Map<String, ResultRow> measurements = new TreeMap<>(
                segments.stream()
                        .parallel()
                        .flatMap(segment -> readMeasurements(fileChannel, segment))
                        .collect(groupingBy(Measurement::station, getMeasurementProcessor())));

        System.out.println(measurements);
    }

    private static Collector<Measurement, MeasurementAggregator, ResultRow> getMeasurementProcessor() {
        return Collector.of(
                MeasurementAggregator::new,
                (a, m) -> {
                    a.min = Math.min(a.min, m.value);
                    a.max = Math.max(a.max, m.value);
                    a.sum += m.value;
                    a.count++;
                },
                (agg1, agg2) -> {
                    agg1.min = Math.min(agg1.min, agg2.min);
                    agg1.max = Math.max(agg1.max, agg2.max);
                    agg1.sum = agg1.sum + agg2.sum;
                    agg1.count = agg1.count + agg2.count;

                    return agg1;
                },
                agg -> new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max));
    }

    private static ArrayList<Segment> getSegments(RandomAccessFile file, int numSegments) throws IOException {
        ArrayList<Segment> segments = new ArrayList<>();
        var fileEnd = file.length();
        var roughStep = fileEnd / numSegments;

        var cursor = 0L;
        for (int i = 0; i < numSegments; i++) {
            var offset = cursor;

            cursor = Math.min(cursor + roughStep, fileEnd);
            // seek up to EOL of the next chunk
            file.seek(cursor);
            while (cursor < fileEnd && file.readByte() != EOL) {
                cursor++;
            }

            if (cursor != fileEnd) {
                // skip over token
                cursor++;
            }
            var length = cursor - offset;

            if (length > 0) {
                segments.add(new Segment(offset, length));
            }

            if (cursor == fileEnd) {
                break;
            } else {
                cursor++; // start next segment
            }
        }

        return segments;
    }

    private static Stream<Measurement> readMeasurements(FileChannel fileChannel, Segment segment) {
        try {
            var iterator = new Iterator<Measurement>() {
                final ByteBuffer bb = fileChannel.map(MapMode.READ_ONLY, segment.offset(), segment.length());

                @Override
                public boolean hasNext() {
                    return bb.hasRemaining();
                }

                @Override
                public Measurement next() {
                    if (!bb.hasRemaining()) {
                        return null;
                    }

                    var offset = bb.position();
                    byte[] readBuffer = new byte[100]; // 100bytes max

                    String station = null;
                    Double reading = null;

                    while (bb.hasRemaining() && (station == null || reading == null)) {
                        var character = bb.get();
                        switch (character) {
                            case SEPARATOR:

                                station = parseStation(readBuffer, bb.position(), offset);
//                                station = new String(Arrays.copyOfRange(readBuffer, 0, bb.position() - offset - 1), StandardCharsets.UTF_8);
                                offset = bb.position();
                                break;

                            case EOL:
                                reading = parseReading(readBuffer, bb.position(), offset);
                                offset = bb.position();
                                break;

                            default:
                                writeToBuffer(readBuffer, bb.position() - offset - 1, character);
//                                readBuffer[bb.position() - offset - 1] = character;
                        }
                    }

                    return new Measurement(station, reading);
                }

            };

            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE | Spliterator.DISTINCT), true);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    // --- START extracted to see operations in profiling ----

    public static Double parseReading(byte[] readBuffer, int position, int offset) {
        return Double
                .parseDouble(new String(Arrays.copyOfRange(readBuffer, 0, position - offset - 1), StandardCharsets.UTF_8));
    }

    public static String parseStation(byte[] readBuffer, int position, int offset) {
        return new String(Arrays.copyOfRange(readBuffer, 0, position - offset - 1), StandardCharsets.UTF_8);
    }

    public static void writeToBuffer(byte[] buffer, int position, byte value) {
        buffer[position] = value;
    }

    // --- END extracted to see operations in profiling ----

    private record Measurement(String station, Double value) {
    }

    private record ResultRow(double min, double mean, double max) {

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private record Segment(Long offset, Long length) {
    }
}
