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

import static java.util.stream.Collectors.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * -- 5: try to create segments v2
 * mean 75.7 ... between (28 s and 200s)!!
 *
 * -- 4: try to create segments
 * mean 240 s
 *
 * -- 3: don't split(;) for arugments
 * mean 75.248 s
 *
 * -- 2: don't create new aggregator
 * mean 83.730 s
 * 
 * -- 1: stream.parallel
 * mean 83.423
 *
 * -- 0: baseline
 * mean 121.768
 */
public class CalculateAverage_xpcoffee {
  private static final String FILE = "./measurements.txt";
  // private static final String FILE =
  // "./src/test/resources/samples/measurements-1.txt";

  private static final char EOL = '\n';
  private static final char SEPARATOR = ';';

  private static record Measurement(String station, Double value) {
  }

  private static record ResultRow(double min, double mean, double max) {

    public String toString() {
      return round(min) + "/" + round(mean) + "/" + round(max);
    }

    private double round(double value) {
      return Math.round(value * 10.0) / 10.0;
    }
  };

  private static class MeasurementAggregator {
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double sum;
    private long count;
  }

  private static record Segment(Long offset, Long length) {
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
      }
    }

    return segments;
  }

  private static Stream<Measurement> readMeasurements(FileChannel fileChannel, Segment segment) {
    try {
      var iterator = new Iterator<Measurement>() {
        ByteBuffer bb = fileChannel.map(MapMode.READ_ONLY, segment.offset(), segment.length());

        @Override
        public boolean hasNext() {
          return bb.hasRemaining();
        };

        @Override
        public Measurement next() {
          try {
            if (!bb.hasRemaining()) {
              return null;
            }

            StringBuilder sb = new StringBuilder();
            var offset = bb.position();

            boolean parsingStation = true;
            byte[] readbuffer = new byte[100]; // 100bytes max

            String station = null;
            Double reading = null;

            while (bb.hasRemaining() && (station == null || reading == null)) {
              var character = bb.get();
              switch (character) {
                case SEPARATOR:
                  station = new String(Arrays.copyOfRange(readbuffer, 0, bb.position() - offset - 1), "UTF-8");
                  parsingStation = false;
                  offset = bb.position();
                  break;

                case EOL:
                  reading = Double
                      .parseDouble(new String(Arrays.copyOfRange(readbuffer, 0, bb.position() - offset - 1), "UTF-8"));
                  offset = bb.position();
                  parsingStation = true;
                  break;

                default:
                  readbuffer[bb.position() - offset - 1] = character;
              }

              // System.out.println("so far" + new String(Arrays.copyOfRange(readbuffer, 0,
              // bb.position() - offset), "UTF-8"));
            }

            return new Measurement(station, reading);

          } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
          }
        };

      };

      return StreamSupport
          .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE | Spliterator.DISTINCT), false);

    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static void main(String[] args) throws IOException {
    Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
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
        agg -> {
          return new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
        });

    var file = new RandomAccessFile(FILE, "r");
    var fileChannel = file.getChannel();
    var threads = Runtime.getRuntime().availableProcessors();
    var segments = getSegments(file, threads);

    Map<String, ResultRow> measurements = new TreeMap<>(
        segments.stream()
            .parallel()
            .flatMap(segment -> readMeasurements(fileChannel, segment))
            .collect(groupingBy(m -> m.station(), collector)));

    System.out.println(measurements);
  }
}
