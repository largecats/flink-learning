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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.*;

public class FraudDetectorJob {

    public static void main(String[] args) throws Exception {
        // Create execution environment for setting job properties, adding sources, and triggering job later
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource()) // Generates an infinite stream of card transactions
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Transaction>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                )
                .name("transactions"); // For debugging purpose

        transactions.print();

        DataStream<Alert> alerts = transactions
                .keyBy(transaction -> transaction.getAccountId()) // Partition a stream by account id
                .process(new FraudDetector()) // Applies the FraudDetector operator to each element in the stream
                .name("fraud-detector");

//        alerts.print();

        alerts
                .addSink(new AlertSink()) // Writes to log with level INFO (for illustration purposes)
                .name("send-alerts");

        env.execute("Fraud Detection"); // Executes the lazily built job
    }
}

/*
Only accountId=3 has fraud transactions.
 */