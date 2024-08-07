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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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

@SuppressWarnings("preview")
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

    /**
     * Split the fileChannel by the number of CPUs
     */
    private static Stream<MemorySegment> splitIntoChunks(final FileChannel fileChannel) throws IOException {
        final var wholeFileSegment = fileChannel.map(MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());
        final var fileSize = fileChannel.size();
        final var chunkSize = fileSize / Runtime.getRuntime().availableProcessors();

        final var chunks = new ArrayList<MemorySegment>();
        long chunkStart = 0L;
        while (chunkStart < fileSize) {
            long chunkEnd = Math.min((chunkStart + chunkSize), fileSize);
            // starting from the calculated chunkEnd, seek the next newline to get the real chunkEnd
            while (chunkEnd < fileSize && wholeFileSegment.getAtIndex(ValueLayout.JAVA_BYTE, chunkEnd) != '\n') {
                chunkEnd++;
            }
            if (chunkEnd < fileSize) {
                chunks.add(wholeFileSegment.asSlice(chunkStart, chunkEnd - chunkStart + 1));
            } else {
                // special case: we are at the end of the file
                chunks.add(wholeFileSegment.asSlice(chunkStart, chunkEnd - chunkStart));
            }
            chunkStart = chunkEnd + 1;
        }
        return chunks.stream();
    }

    /**
     * Read lines from the MemorySegment
     */
    private static Stream<Measurement> readMeasurements(MemorySegment segment) {
        final byte[] buffer = new byte[100];

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<>() {
            long position = 0;

            @Override
            public boolean hasNext() {
                return position < segment.byteSize();
            }

            @Override
            public Measurement next() {
                var station = readString(';');
                var value = readInt('\n');

                return new Measurement(station, value);
            }

            private String readString(char endChar) {
                var index = 0;
                var currentChar = segment.get(ValueLayout.JAVA_BYTE, position++);
                while (currentChar != endChar) {
                    buffer[index] = currentChar;
                    index++;
                    currentChar = segment.get(ValueLayout.JAVA_BYTE, position++);
                }
                return new String(buffer, 0, index, StandardCharsets.UTF_8);
            }

            private int readInt(char endChar) {
                var negative = false;
                var value = 0;

                var currentChar = segment.get(ValueLayout.JAVA_BYTE, position++);
                if (currentChar == '-') {
                    negative = true;
                    currentChar = segment.get(ValueLayout.JAVA_BYTE, position++);
                }
                while (currentChar != endChar) {
                    if (currentChar != '.') {
                        value = (value * 10) + (currentChar - '0');
                    }
                    currentChar = segment.get(ValueLayout.JAVA_BYTE, position++);
                }
                if (negative) {
                    value *= -1;
                }
                return value;
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
