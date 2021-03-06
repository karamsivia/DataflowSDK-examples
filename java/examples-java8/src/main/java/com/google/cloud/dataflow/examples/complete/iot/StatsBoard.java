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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import com.google.cloud.dataflow.examples.common.ExampleOptions;
import com.google.cloud.dataflow.examples.common.ExampleUtils;
import com.google.cloud.dataflow.examples.complete.iot.utils.WriteToBigQuery;
import com.google.cloud.dataflow.examples.complete.iot.utils.WriteWindowedToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * <p>This pipeline processes an unbounded stream of 'iot events'. This pipeline uses
 * an unbounded data source, and  provides speculative results, and allows
 * handling of late data, at much lower latency. We can use the early/speculative results to keep a
 * 'StatsBoard' updated in near-realtime. Our handling of late data lets us generate correct
 * results. We're now outputting window results as they're
 * calculated, giving us much lower latency than with the previous batch examples.
 *
 * <p>Run {@code injector.Injector} to generate pubsub data for this pipeline.  The Injector
 * documentation provides more detail on how to do this.
 *
 * <p>To execute this pipeline, specify the pipeline configuration like this:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --tempLocation=gs://YOUR_TEMP_DIRECTORY
 *   --runner=YOUR_RUNNER
 *   --dataset=YOUR-DATASET
 *   --topic=projects/YOUR-PROJECT/topics/YOUR-TOPIC
 * }
 * </pre>
 *
 * <p>The BigQuery dataset you specify must already exist. The PubSub topic you specify should be
 * the same topic to which the Injector is publishing.
 */
public class StatsBoard extends HourlyHubStats {

  private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";

  private static DateTimeFormatter fmt =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
  static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  static final Duration TEN_MINUTES = Duration.standardMinutes(10);


  /**
   * Options supported by {@link StatsBoard}.
   */
  interface Options extends HourlyHubStats.Options, ExampleOptions, StreamingOptions {

    @Description("BigQuery Dataset to write tables to. Must already exist.")
    @Validation.Required
    String getDataset();
    void setDataset(String value);

    @Description("Pub/Sub topic to read from")
    @Validation.Required
    String getTopic();
    void setTopic(String value);

    @Description("Numeric value of fixed window duration for Hub analysis, in minutes")
    @Default.Integer(60)
    Integer getHubWindowDuration();
    void setHubWindowDuration(Integer value);

    @Description("Numeric value of allowed data lateness, in minutes")
    @Default.Integer(120)
    Integer getAllowedLateness();
    void setAllowedLateness(Integer value);

    @Description("Prefix used for the BigQuery table names")
    @Default.String("stats")
    String getStatsBoardTableName();
    void setStatsBoardTableName(String value);
  }

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write Hub Stats sums and includes event timing information.
   */
  protected static Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>
      configureWindowedTableWrite() {

    Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        new HashMap<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>();
    tableConfigure.put(
        "Hub",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_stats",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "INTEGER", (c, w) -> c.element().getValue()));
    tableConfigure.put(
        "window_start",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING",
            (c, w) -> {
              IntervalWindow window = (IntervalWindow) w;
              return fmt.print(window.start());
            }));
    tableConfigure.put(
        "processing_time",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> fmt.print(Instant.now())));
    tableConfigure.put(
        "timing",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> c.pane().getTiming().toString()));
    return tableConfigure;
  }


  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is passed to the {@link WriteToBigQuery} constructor to write Upc Stats sums.
   */
  protected static Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>>
  configureBigQueryWrite() {
    Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        new HashMap<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>>();
    tableConfigure.put(
        "UPC",
        new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_stats",
        new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
            "INTEGER", (c, w) -> c.element().getValue()));
    return tableConfigure;
  }


  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write UPC Stats .
   */
  protected static Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>>
      configureGlobalWindowBigQueryWrite() {

    Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        configureBigQueryWrite();
    tableConfigure.put(
        "processing_time",
        new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> fmt.print(Instant.now())));
    return tableConfigure;
  }


  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    ExampleUtils exampleUtils = new ExampleUtils(options);
    Pipeline pipeline = Pipeline.create(options);

    // Read iot events from Pub/Sub using custom timestamps, which are extracted from the pubsub
    // data elements, and parse the data.
    PCollection<IotActionInfo> iotEvents = pipeline
        .apply(PubsubIO.readStrings()
            .withTimestampAttribute(TIMESTAMP_ATTRIBUTE).fromTopic(options.getTopic()))
        .apply("ParseIotEvent", ParDo.of(new ParseEventFn()));

    iotEvents
        .apply(
            "CalculateHubStats",
            new CalculateHubStats(
                Duration.standardMinutes(options.getHubWindowDuration()),
                Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply(
            "WriteHubStatsSums",
            new WriteWindowedToBigQuery<KV<String, Integer>>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getStatsBoardTableName() + "_Hub",
                configureWindowedTableWrite()));
    iotEvents
        .apply(
            "CalculateUpcStats",
            new CalculateUpcStats(Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply(
            "WriteUpcStatsSums",
            new WriteToBigQuery<KV<String, Integer>>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getStatsBoardTableName() + "_Upc",
                configureGlobalWindowBigQueryWrite()));

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
    exampleUtils.waitToFinish(result);
  }

  /**
   * Calculates Stats for each Hub within the configured window duration.
   */
  // [START DocInclude_WindowAndTrigger]
  // Extract Hub/Stat pairs from the event stream, using hour-long windows by default.
  @VisibleForTesting
  static class CalculateHubStats
      extends PTransform<PCollection<IotActionInfo>, PCollection<KV<String, Integer>>> {
    private final Duration hubWindowDuration;
    private final Duration allowedLateness;

    CalculateHubStats(Duration hubWindowDuration, Duration allowedLateness) {
      this.hubWindowDuration = hubWindowDuration;
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<IotActionInfo> infos) {
      return infos.apply("StatsBoardHubFixedWindows",
          Window.<IotActionInfo>into(FixedWindows.of(hubWindowDuration))
              // We will get early (speculative) results as well as cumulative
              // processing of late data.
              .triggering(AfterWatermark.pastEndOfWindow()
                  .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                      .plusDelayOf(FIVE_MINUTES))
                  .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
                      .plusDelayOf(TEN_MINUTES)))
              .withAllowedLateness(allowedLateness)
              .accumulatingFiredPanes())
          // Extract and sum Hubname/Stat pairs from the event data.
          .apply("ExtractHubStats", new ExtractAndSumStats("hub"));
    }
  }
  // [END DocInclude_WindowAndTrigger]

  // [START DocInclude_ProcTimeTrigger]
  /**
   * Extract Upc/Stats pairs from the event stream using processing time, via global windowing.
   * Get periodic updates on all Upcs' running Statss.
   */
  @VisibleForTesting
  static class CalculateUpcStats
      extends PTransform<PCollection<IotActionInfo>, PCollection<KV<String, Integer>>> {
    private final Duration allowedLateness;

    CalculateUpcStats(Duration allowedLateness) {
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<IotActionInfo> input) {
      return input.apply("StatsBoardUpcGlobalWindow",
          Window.<IotActionInfo>into(new GlobalWindows())
              // Get periodic results every ten minutes.
              .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                  .plusDelayOf(TEN_MINUTES)))
              .accumulatingFiredPanes()
              .withAllowedLateness(allowedLateness))
          // Extract and sum Upcname/Stat pairs from the event data.
          .apply("ExtractUpcStats", new ExtractAndSumStats("UPC"));
    }
  }
  // [END DocInclude_ProcTimeTrigger]
}
