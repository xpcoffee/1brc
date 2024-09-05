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
import java.util.Arrays;
import java.util.Map;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingBy;

/**
 * -- 8: custom spliterator
 * 1.739 s ±  0.384 s !!
 * <p>
 * this is without optimizing types and aggregation; just the reading optimization
 * <p>
 * -- 7: arrays of measurements rather than an iterator; chunk by sizes
 * mean 38.237 s ±  4.597 s
 * <p>
 * much more consistent times but ooms with less memory
 *
 * <p>
 * -- 6: allow parallel on readMeasurements spliterator ; got faster ver time (CPU caching I think)
 * mean 33.281
 * Range (min  max):   27.912 s  37.837 s
 * <p>
 * lots of "self time" in profile; looks like iterator parallelism getting in the way
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
    private static final String DEFAULT_FILE_PATH = "./measurements.txt";

    private static final char EOL = '\n';
    private static final char SEPARATOR = ';';

    public static void main(String[] args) throws IOException {
        String filePath = args.length > 0 ? args[0] : DEFAULT_FILE_PATH;

        var file = new RandomAccessFile(filePath, "r");
        var fileChannel = file.getChannel();
        // var threads = Runtime.getRuntime().availableProcessors();
        // var chunkSize = (int) file.length() / threads;
        // var chunkSize = 1024 * 60; // 10mb

        Map<String, ResultRow> measurements = new TreeMap<>(
                StreamSupport.stream(new SegmentSpliterator(fileChannel, 0, (int) fileChannel.size()), true)
                        // .parallel()
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

    private static class SegmentSpliterator implements Spliterator<Measurement> {
        private final FileChannel fileChannel;
        private final ByteBuffer memoryMappedFile;
        private final byte[] readBuffer = new byte[100]; // 100bytes max for the name of the station
        private final int fileCursor;

        SegmentSpliterator(FileChannel fileChannel, int fileCursor, int length) throws IOException {
            this.fileChannel = fileChannel;
            this.fileCursor = fileCursor;
            memoryMappedFile = fileChannel.map(MapMode.READ_ONLY, fileCursor, length);
        }

        @Override
        public boolean tryAdvance(Consumer<? super Measurement> action) {
            String station = null;
            var initialOffset = memoryMappedFile.position();
            var offset = initialOffset;

            while (memoryMappedFile.hasRemaining()) {
                var character = memoryMappedFile.get();
                switch (character) {
                    case SEPARATOR:
                        station = parseStation(readBuffer, memoryMappedFile.position(), offset);
                        offset = memoryMappedFile.position();
                        break;

                    case EOL:
                        var reading = parseReading(readBuffer, memoryMappedFile.position(), offset);
                        action.accept(new Measurement(station, reading));
                        return true;

                    default:
                        writeToBuffer(
                                readBuffer,
                                memoryMappedFile.position() - offset - 1, // -1 because position increase in get, but we want original position
                                character);
                }
            }
            return false;
        }

        private void printOutRecord(int offset) {
            var initialPosition = memoryMappedFile.position();
            memoryMappedFile.position(offset);
            byte[] bytes = new byte[memoryMappedFile.limit() - offset];

            var pos = memoryMappedFile.position() - offset;
            while (memoryMappedFile.hasRemaining() && bytes[pos] != EOL) {
                pos = memoryMappedFile.position() - offset;
                bytes[pos] = memoryMappedFile.get();
            }

            System.out.println(new String(Arrays.copyOfRange(bytes, 0, memoryMappedFile.position() - offset - 1), StandardCharsets.UTF_8));
            memoryMappedFile.position(initialPosition);
        }

        private void printOutFullRange() {
            var initialPosition = memoryMappedFile.position();
            memoryMappedFile.position(0);
            byte[] bytes = new byte[memoryMappedFile.limit()];

            while (memoryMappedFile.hasRemaining()) {
                var pos = memoryMappedFile.position();
                bytes[pos] = memoryMappedFile.get();
            }

            System.out.println(new String(Arrays.copyOfRange(bytes, 0, memoryMappedFile.position() - 1), StandardCharsets.UTF_8));
            memoryMappedFile.position(initialPosition);
        }

        @Override
        public Spliterator<Measurement> trySplit() {
            if (memoryMappedFile.remaining() < 100) { // smallest valid record is 8 e.g. "aaaa;0.0"
                return null;
            }

            var newLimit = Math.ceilDiv(memoryMappedFile.remaining(), 2);
            while (memoryMappedFile.get(newLimit - 1) != EOL) { // find closest newline
                newLimit--;
                if (newLimit == 0) {
                    return null;
                }
            }

            try {
                var newSplit = new SegmentSpliterator(fileChannel, fileCursor + newLimit, memoryMappedFile.limit() - newLimit);
                this.memoryMappedFile.limit(newLimit);
                return newSplit;
            } catch (IOException ex) {
                return null;
            }
        }

        @Override
        public long estimateSize() {
            return this.memoryMappedFile.remaining();
        }

        @Override
        public int characteristics() {
            return ORDERED | SIZED | SUBSIZED | CONCURRENT | IMMUTABLE | NONNULL;
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
