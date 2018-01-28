/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.examples.complete.iot;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import com.google.cloud.dataflow.examples.complete.iot.utils.WriteToText;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * This class is the second in a series of four pipelines that tell a story in a 'gaming'
 * domain, following {@link UPC}. In addition to the concepts introduced in {@link UPC},
 * new concepts include: windowing and element timestamps; use of {@code Filter.by()}.
 *
 * <p>This pipeline processes data collected from gaming events in batch, building on {@link
 * UPC} but using fixed windows. It calculates the sum of Statss per UPC, for each window,
 * optionally allowing specification of two timestamps before and after which data is filtered out.
 * This allows a model where late data collected after the intended analysis window can be included,
 * and any late-arriving data prior to the beginning of the analysis window can be removed as well.
 * By using windowing and adding element timestamps, we can do finer-grained analysis than with the
 * {@link UPC} pipeline. However, our batch processing is high-latency, in that we don't get
 * results from plays at the beginning of the batch's time period until the batch is processed.
 *
 * <p>To execute this pipeline, specify the pipeline configuration like this:
 * <pre>{@code
 *   --tempLocation=YOUR_TEMP_DIRECTORY
 *   --runner=YOUR_RUNNER
 *   --output=YOUR_OUTPUT_DIRECTORY
 *   (possibly options specific to your runner or permissions for your temp/output locations)
 * }
 * </pre>
 *
 * <p>Optionally include {@code --input} to specify the batch input file path.
 * To indicate a time after which the data should be filtered out, include the
 * {@code --stopMin} arg. E.g., {@code --stopMin=2015-10-18-23-59} indicates that any data
 * timestamped after 23:59 PST on 2015-10-18 should not be included in the analysis.
 * To indicate a time before which data should be filtered out, include the {@code --startMin} arg.
 * If you're using the default input specified in {@link UPC},
 * "gs://apache-beam-samples/iot/gaming_data*.csv", then
 * {@code --startMin=2015-11-16-16-10 --stopMin=2015-11-17-16-10} are good values.
 */
public class HourlyHubStats extends UPC {

  private static DateTimeFormatter fmt =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
  private static DateTimeFormatter minFmt =
      DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));


  /**
   * Options supported by {@link HourlyHubStats}.
   */
  interface Options extends UPC.Options {

    @Description("Numeric value of fixed window duration, in minutes")
    @Default.Integer(60)
    Integer getWindowDuration();
    void setWindowDuration(Integer value);

    @Description("String representation of the first minute after which to generate results,"
        + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
        + "Any input data timestamped prior to that minute won't be included in the sums.")
    @Default.String("1970-01-01-00-00")
    String getStartMin();
    void setStartMin(String value);

    @Description("String representation of the first minute for which to not generate results,"
        + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
        + "Any input data timestamped after that minute won't be included in the sums.")
    @Default.String("2100-01-01-00-00")
    String getStopMin();
    void setStopMin(String value);
  }

  /**
   * Create a map of information that describes how to write pipeline output to text. This map
   * is passed to the {@link WriteToText} constructor to write UPC Stats sums and
   * includes information about window start time.
   */
  protected static Map<String, WriteToText.FieldFn<KV<String, Integer>>>
      configureOutput() {
    Map<String, WriteToText.FieldFn<KV<String, Integer>>> config =
        new HashMap<String, WriteToText.FieldFn<KV<String, Integer>>>();
    config.put("UPC", (c, w) -> c.element().getKey());
    config.put("total_count", (c, w) -> c.element().getValue());
    config.put(
        "window_start",
        (c, w) -> {
              IntervalWindow window = (IntervalWindow) w;
              return fmt.print(window.start());
            });
    return config;
  }


  /**
   * Run a batch pipeline to do windowed analysis of the data.
   */
  // [START DocInclude_HTSMain]
  public static void main(String[] args) throws Exception {
    // Begin constructing a pipeline configured by commandline flags.
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    final Instant stopMinTimestamp = new Instant(minFmt.parseMillis(options.getStopMin()));
    final Instant startMinTimestamp = new Instant(minFmt.parseMillis(options.getStartMin()));

    // Read 'gaming' events from a text file.
    pipeline.apply(TextIO.read().from(options.getInput()))
      // Parse the incoming data.
      .apply("ParseIotEvent", ParDo.of(new ParseEventFn()))

      // Filter out data before and after the given times so that it is not included
      // in the calculations. As we collect data in batches (say, by day), the batch for the day
      // that we want to analyze could potentially include some late-arriving data from the previous
      // day. If so, we want to weed it out. Similarly, if we include data from the following day
      // (to scoop up late-arriving events from the day we're analyzing), we need to weed out events
      // that fall after the time period we want to analyze.
      // [START DocInclude_HTSFilters]
      .apply("FilterStartTime", Filter.by(
          (IotActionInfo gInfo)
              -> gInfo.getTimestamp() > startMinTimestamp.getMillis()))
      .apply("FilterEndTime", Filter.by(
          (IotActionInfo gInfo)
              -> gInfo.getTimestamp() < stopMinTimestamp.getMillis()))
      // [END DocInclude_HTSFilters]

      // [START DocInclude_HTSAddTsAndWindow]
      // Add an element timestamp based on the event log, and apply fixed windowing.
      .apply("AddEventTimestamps",
             WithTimestamps.of((IotActionInfo i) -> new Instant(i.getTimestamp())))
      .apply("FixedWindowsUPC", Window.<IotActionInfo>into(
          FixedWindows.of(Duration.standardMinutes(options.getWindowDuration()))))
      // [END DocInclude_HTSAddTsAndWindow]

      // Extract and sum UPCname/Stats pairs from the event data.
      .apply("ExtractHubStats", new ExtractAndSumStats("upc"))
      .apply("WriteHubStatsSums",
          new WriteToText<KV<String, Integer>>(
              options.getOutput(),
              configureOutput(),
              true));

    pipeline.run().waitUntilFinish();
  }
  // [END DocInclude_HTSMain]

}
