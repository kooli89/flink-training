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

package flinkexample;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.source.TransactionSource;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class T1MainJob {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		StreamExecutionEnvironment env =  StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions");

		DataStream<Transaction> processing = transactions
				.keyBy(Transaction::getAccountId)// this part is important just be aware
				// each grouped transaction will be processed.
				.process(new T1())
				.name("process-transactions");

		try {
			env.execute("Flink Fraud Detection Example");
		} catch (Exception e) {
			System.out.println(e);
		}

	}
}