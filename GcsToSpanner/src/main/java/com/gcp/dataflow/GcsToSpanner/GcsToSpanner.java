/*
 * Copyright 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gcp.dataflow.GcsToSpanner;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class GcsToSpanner {
    private static final Logger LOG = LoggerFactory.getLogger(GcsToSpanner.class);

    public interface Options extends PipelineOptions {

        @Description("Instance ID for Cloud Spanner")
        @Validation.Required
        ValueProvider<String> getInstanceId();

        void setInstanceId(ValueProvider<String> value);

        @Description("Database ID for Cloud Spanner")
        @Validation.Required
        ValueProvider<String> getDatabaseId();

        void setDatabaseId(ValueProvider<String> value);

        @Description("InputBucket")
        @Validation.Required
        ValueProvider<String> getInputBucket();

        void setInputBucket(ValueProvider<String> out)

        @Description("Spanner host")
        @Validation.Required
        ValueProvider<String> getSpannerHost();

        void setSpannerHost(ValueProvider<String> value);
    }

    /**
     * Runs the pipeline with the supplied options
     * 
     * @param options are execution parameters to the pipeline.
     * @return The result of the pipeline execution.
     */
    private static PipelineResult run(Options options) {

        Pipeline pipeline = Pipeline.create(options);

        SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(options.getSpannerProjectId())
            .withHost(options.getSpannerHost())
            .withInstanceId(options.getInstanceId())
            .withDatabaseId(options.getDatabaseId());

        PCollection<String> data = pipeline.apply("")
         

        #TODO : here we have to generate the graph of the pipeline



        pipeline.run().waitUntilFinish();
    }

     /**
      * main entry point for executing the pipeline.
      *
      * @param the CLI arguments to the pipeline
      */
    public static void main(String[] args) throws Exception {
        LOG.info("Starting gcs to spanner ingestion pipeline")

        PipelineOptionsFactory.register(Options.class)
        Options options = PipelineOptionsFactory.fromArs(args).WithValidation().as(Options.class)      
         
        run(options)
    }
}
