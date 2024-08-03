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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CalculateAverage_baseline {
    private static final Path FILE = Paths.get("./measurements.txt");

    public static void main(String[] args) throws IOException {
        var channel = FileChannel.open(FILE, StandardOpenOption.READ);
        Map<String, Result> measurements = splitIntoChunks(channel)
                .parallel()
                .flatMap(CalculateAverage_baseline::readLines)
                .map(line -> {
                    int separatorPos = line.indexOf(";");
                    return new Measurement(line.substring(0, separatorPos), Double.parseDouble(line.substring(separatorPos + 1)));
                })
                .collect(Collectors.groupingBy(Measurement::station, Collector.of(
                        AggregatedMeasurements::new,
                        (agg, measurement) -> {
                            agg.min = Math.min(agg.min, measurement.value);
                            agg.max = Math.max(agg.max, measurement.value);
                            agg.sum += measurement.value;
                            agg.count++;
                        },
                        (agg1, agg2) -> {
                            var agg = new AggregatedMeasurements();
                            agg.min = Math.min(agg1.min, agg2.min);
                            agg.max = Math.max(agg1.max, agg2.max);
                            agg.sum = agg1.sum + agg2.sum;
                            agg.count = agg1.count + agg2.count;
                            return agg;
                        },
                        agg -> new Result(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max))));
        Map<String, Result> sorted = new TreeMap<>(measurements);
        System.out.println(sorted);
    }

    private static Stream<ByteBuffer> splitIntoChunks(final FileChannel fileChannel) throws IOException {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            private static final long CHUNK_SIZE = 10L * 1024 * 1024;

            private final long size = fileChannel.size();
            private long position = 0;

            @Override
            public boolean hasNext() {
                return position < size;
            }

            @Override
            public ByteBuffer next() {
                try {
                    var byteBuffer = fileChannel.map(MapMode.READ_ONLY, position, Math.min(CHUNK_SIZE, size - position));

                    // don't split the data in the middle of lines
                    // find the closest previous newline
                    int realEnd = byteBuffer.limit() - 1;
                    while (byteBuffer.get(realEnd) != '\n') {
                        realEnd--;
                    }
                    realEnd++;

                    position += realEnd;
                    return byteBuffer.limit(realEnd);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            }
        }, Spliterator.IMMUTABLE), true);

    }

    private static Stream<String> readLines(ByteBuffer byteBuffer) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {

            @Override
            public boolean hasNext() {
                return byteBuffer.hasRemaining();
            }

            @Override
            public String next() {
                ByteBuffer string = byteBuffer.slice();
                int length = 0;
                while (byteBuffer.hasRemaining() && byteBuffer.get() != '\n') {
                    length++;
                }
                string.limit(length);
                return StandardCharsets.UTF_8.decode(string).toString();
            }
        }, Spliterator.IMMUTABLE), true);
    }

    private record Measurement(String station, double value) {
    }

    private static class AggregatedMeasurements {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    private record Result(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
