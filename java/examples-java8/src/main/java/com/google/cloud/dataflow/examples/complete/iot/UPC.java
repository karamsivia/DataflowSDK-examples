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
import org.apache.avro.reflect.Nullable;
import com.google.cloud.dataflow.examples.complete.iot.utils.WriteToText;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>This pipeline does batch processing of data collected from iot events. It calculates the
 * Stats per UPC (total item count per UPC), over an entire batch of iot data (collected, say, for each day). The
 * batch processing will not include any late data that arrives after the day's cutoff point.
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
 * <p>Optionally include the --input argument to specify a batch input file.
 * See the --input default value for example batch data file, or use {@code injector.Injector} to
 * generate your own batch data.
  */
public class UPC {

  /**
   * Class to hold info about a iot event.
   */
  @DefaultCoder(AvroCoder.class)
  static class IotActionInfo {
    @Nullable String upc;
    @Nullable String hub;
    @Nullable String scan;
    @Nullable String store;
    @Nullable Integer stats;
    @Nullable Long timestamp;

    public IotActionInfo() {}

    public IotActionInfo(String scan, String upc, String hub,String store, Long timestamp) {
      this.upc = upc;
      this.hub = hub;
      this.scan = scan;
      this.store = store;
      this.stats = 1;
      this.timestamp = timestamp;
    }

    public String getUpc() {
      return this.upc;
    }
    public String getHub() {
      return this.hub;
    }
    public String getScan() {
      return this.scan;
    }
    public String getStore() {
      return this.store;
    }
    public Integer getStats() {
      return this.stats;
    }

   
    public String getKey(String keyname) {
      if (keyname.equals("hub")) {
        return this.hub;
      } else if (keyname.equals("scan")) {
        return this.scan;
      } else if (keyname.equals("store")) {
        return this.store;
      } else {  // return upcname as default
        return this.upc;
      }
    }
    public Long getTimestamp() {
      return this.timestamp;
    }
  }


  /**
   * Parses the raw Iot event info into IotActionInfo objects. Each event line has the following
   * format: scan,upcname,hubname,store,timestamp_in_ms
   * 
   */
  static class ParseEventFn extends DoFn<String, IotActionInfo> {

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
    private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] components = c.element().split(",");
      try {
        String scan = components[0].trim();
        String upc = components[1].trim();
        String hub = components[2].trim();
        String store = components[3].trim();
//        Integer stats = Integer.parseInt(components[2].trim());
        Long timestamp = Long.parseLong(components[4].trim());
        IotActionInfo gInfo = new IotActionInfo(scan,upc, hub, store, timestamp);
        c.output(gInfo);
      } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
        numParseErrors.inc();
        LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
      }
    }
  }

  /**
   * A transform to extract key/stats information from IotActionInfo, and sum the stats. The
   * constructor arg determines whether 'hub' or 'upc' info is extracted.
   */
  // [START DocInclude_USExtractXform]
  public static class ExtractAndSumStats
      extends PTransform<PCollection<IotActionInfo>, PCollection<KV<String, Integer>>> {

    private final String field;

    ExtractAndSumStats(String field) {
      this.field = field;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(
        PCollection<IotActionInfo> iotInfo) {

      return iotInfo
        .apply(MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
            .via((IotActionInfo gInfo) -> KV.of(gInfo.getKey(field), gInfo.getStats())))
        .apply(Sum.<String>integersPerKey());
    }
  }
  // [END DocInclude_USExtractXform]


  /**
   * Options supported by {@link upcStats}.
   */
  public interface Options extends PipelineOptions {

    @Description("Path to the data file(s) containing Iot data.")
    // The default maps to two large Google Cloud Storage files (each ~12GB) holding two subsequent
    // day's worth (roughly) of data.
    @Default.String("gs://apache-beam-samples/Iot/gaming_data*.csv")
    String getInput();
    void setInput(String value);

    // Set this required option to specify where to write the output.
    @Description("Path of the file to write to.")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
  }

  /**
   * Create a map of information that describes how to write pipeline output to text. This map
   * is passed to the {@link WriteToText} constructor to write upc Stats sums.
   */
  protected static Map<String, WriteToText.FieldFn<KV<String, Integer>>>
      configureOutput() {
    Map<String, WriteToText.FieldFn<KV<String, Integer>>> config =
        new HashMap<String, WriteToText.FieldFn<KV<String, Integer>>>();
    config.put("upc", (c, w) -> c.element().getKey());
    config.put("total_stats", (c, w) -> c.element().getValue());
    return config;
  }

  /**
   * Run a batch pipeline.
   */
 // [START DocInclude_USMain]
  public static void main(String[] args) throws Exception {
    // Begin constructing a pipeline configured by commandline flags.
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Read events from a text file and parse them.
    pipeline
        .apply(TextIO.read().from(options.getInput()))
        .apply("ParseIotEvent", ParDo.of(new ParseEventFn()))
        // Extract and sum Upcname/stats pairs from the event data.
        .apply("ExtractUpcStats", new ExtractAndSumStats("upc"))
        .apply(
            "WriteUpcStatsSums",
            new WriteToText<KV<String, Integer>>(
                options.getOutput(),
                configureOutput(),
                false));

    // Run the batch pipeline.
    pipeline.run().waitUntilFinish();
  }
  // [END DocInclude_USMain]
}
