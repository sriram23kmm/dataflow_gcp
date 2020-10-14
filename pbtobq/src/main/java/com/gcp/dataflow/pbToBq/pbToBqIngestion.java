/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.gcp.dataflow.pbToBq;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * A dataflow pipeline that listens to a PubSub topic and writes out to BigQuery
 *
 */
public class pbToBqIngestion {
    private static final Logger LOG = LoggerFactory.getLogger(pbToBqIngestion.class);

    // Define two TupleTags, one for each output.
    static final TupleTag<TableRow> PARSED = new TupleTag<TableRow>() {
        private static final long serialVersionUID = -3666400605327103663L;
    };
    static final TupleTag<TableRow> REJECTED = new TupleTag<TableRow>() {
        private static final long serialVersionUID = -6521710580269266936L;
    };

    public interface Options extends StreamingOptions {

        @Description("Input topic")
        @Validation.Required
        ValueProvider<String> getInput();

        void setInput(ValueProvider<String> s);

        @Description("Output BigQuery table <project_id>:<dataset_id>.<table_id>")
        @Validation.Required
        ValueProvider<String> getOutput();

        void setOutput(ValueProvider<String> s);

        @Description("Rejected BigQuery table <project_id>:<dataset_id>.<table_id>")
        @Validation.Required
        ValueProvider<String> getRejected();

        void setRejected(ValueProvider<String> s);

        @Description("Load number")
        @Default.Integer(1)
        @Validation.Required
        ValueProvider<Integer> getLoadNumber();

        void setLoadNumber(ValueProvider<Integer> i);
    }

    /**
     * Main entry point for executing the pipeline.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) throws Exception {
        LOG.info("Starting ingestion pipeline");

        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);

        run(options);
    }

    private static TableSchema rejectedTableSchema() {
        // Build the table schema for the rejected table.
        List<TableFieldSchema> rejectedFields = new ArrayList<>();
        rejectedFields.add(new TableFieldSchema().setName("MESSAGE").setType("STRING"));
        rejectedFields.add(new TableFieldSchema().setName("EXCEPTION").setType("STRING"));
        rejectedFields.add(new TableFieldSchema().setName("LOAD_NUMBER").setType("INTEGER"));
        rejectedFields.add(new TableFieldSchema().setName("LOAD_TIMESTAMP").setType("TIMESTAMP"));
        return new TableSchema().setFields(rejectedFields);
    }

    /**
     * Runs the pipeline with the supplied options.
     *
     * @param options The execution parameters to the pipeline.
     * @return The result of the pipeline execution.
     */
    private static PipelineResult run(Options options) {

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> messages = pipeline
                .apply("GetMessages", PubsubIO.readStrings().fromSubscription(options.getInput()));

        PCollectionTuple mixedCollection = messages
                .apply("ConvertMessageToTableRow", new PubsubMessageToTableRow(options));

        // Get subset of the output with tag parsed.
        mixedCollection.get(PARSED)
                .apply("WriteParsedToBQ", BigQueryIO.writeTableRows().to(options.getOutput())
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        // Get subset of the output with tag notParsed.
        mixedCollection.get(REJECTED)
                .apply("WriteRejectedToBQ", BigQueryIO.writeTableRows().to(options.getRejected())
                        .withSchema(rejectedTableSchema())//
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

        return pipeline.run();
    }

    static class PubsubMessageToTableRow
            extends PTransform<PCollection<String>, PCollectionTuple> {

        private static final long serialVersionUID = -1901854581332637254L;
        private final Options options;

        PubsubMessageToTableRow(Options options) {
            this.options = options;
        }

        @Override
        public PCollectionTuple expand(PCollection<String> messages) {

            PCollectionTuple mixedCollection = messages
                    .apply("ParseMessages", ParDo.of(new ParsePubSubMessage(PARSED, REJECTED))
                            .withOutputTags(PARSED, TupleTagList.of(REJECTED)));

            // Return a single PCollectionTuple
            return mixedCollection;
        }
    }

    static class ParsePubSubMessage extends DoFn<String, TableRow> implements Serializable {

        private static final long serialVersionUID = 6365363605692640030L;
        private final TupleTag<TableRow> parsed;
        private final TupleTag<TableRow> notParsed;

        public ParsePubSubMessage(TupleTag<TableRow> parsed,
                                  TupleTag<TableRow> notParsed) {
            this.parsed = parsed;
            this.notParsed = notParsed;
        }

        /**
         * Converts a string to a {@link TableRow} object. If the data fails to convert, a {@link
         * RuntimeException} will be thrown.
         *
         * @param message The message string to parse.
         * @return The parsed {@link TableRow} object.
         */
        public TableRow convertMessageToTableRow(String message, Options pipelineOptions) throws Exception {

            // Try to parse the message into a TableRow object.
			
            TableRow row;
			try (InputStream inputStream = new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8))) 
				 {
				row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
                 } 
			catch (IOException e) {
				
				throw new InvalidContentException("Unexpected message content");
            }
			row.set("LOAD_TIMESTAMP", Instant.now().getEpochSecond());
			
            return row;
         }

        @ProcessElement
        public void processElement(ProcessContext c, MultiOutputReceiver out) throws Exception {
            Options pipelineOptions = c.getPipelineOptions().as(Options.class);

            try {
                TableRow row = convertMessageToTableRow(c.element(), pipelineOptions);
                out.get(parsed).output(row);
                // If we catch RuntimeException then it doesn't catch SAXParseException Content is not allowed in prolog
            } catch (Exception e) {
                TableRow notParsedRow = new TableRow();
                notParsedRow.set("MESSAGE", c.element());
                notParsedRow.set("EXCEPTION", e.toString());
                notParsedRow.set("LOAD_NUMBER", pipelineOptions.getLoadNumber());
                notParsedRow.set("LOAD_TIMESTAMP", Instant.now().getEpochSecond());
                out.get(notParsed).output(notParsedRow);
            }

        }
    }

    public static class InvalidContentException extends RuntimeException {

        private static final long serialVersionUID = -3114619551537513712L;

        public InvalidContentException(String message) {
            super(message);
        }
    }
}