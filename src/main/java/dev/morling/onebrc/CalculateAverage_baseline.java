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
                .flatMap(CalculateAverage_baseline::readMeasurements)
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
                        agg -> new Result(agg.min, (1.0 * agg.sum / agg.count), agg.max))));
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

    private static Stream<Measurement> readMeasurements(ByteBuffer byteBuffer) {
        final byte[] buffer = new byte[100];

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {

            @Override
            public boolean hasNext() {
                return byteBuffer.hasRemaining();
            }

            @Override
            public Measurement next() {
                var station = readString(';');
                var value = readStringWithoutDot('\n');

                return new Measurement(station, Integer.parseInt(value));
            }

            private String readString(char endChar) {
                var index = 0;
                var currentChar = byteBuffer.get();
                while (currentChar != endChar) {
                    buffer[index] = currentChar;
                    index++;
                    currentChar = byteBuffer.get();
                }
                return new String(buffer, 0, index, StandardCharsets.UTF_8);
            }

            private String readStringWithoutDot(char endChar) {
                var index = 0;
                var currentChar = byteBuffer.get();
                while (currentChar != endChar) {
                    if (currentChar != '.') {
                        buffer[index] = currentChar;
                        index++;
                    }
                    currentChar = byteBuffer.get();
                }
                return new String(buffer, 0, index, StandardCharsets.UTF_8);
            }
        }, Spliterator.IMMUTABLE), true);
    }

    private record Measurement(String station, int value) {
    }

    private static class AggregatedMeasurements {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int sum;
        private long count;
    }

    private record Result(double min, double mean, double max) {
        public String toString() {
            return min / 10.0 + "/" + Math.round(mean) / 10.0 + "/" + max / 10.0;
        }
    }
}
