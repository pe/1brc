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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class CalculateAverage_baseline {
    private static final Path FILE = Paths.get("./measurements.txt");

    public static void main(String[] args) throws IOException {
        Map<String, Result> measurements = Files.lines(FILE)
                .parallel()
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
