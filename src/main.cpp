// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

#include <aws/core/Aws.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/crypto/Sha256.h>
#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>

using namespace std;

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::CompactionServiceOptionsOverride;

string kDBPath = "/home/ubuntu/s3fuse/conn.db";
string kDBCompactionOutputPath = "/home/ubuntu/s3fuse/conn.db/output";
string kCompactionRequestQueueUrl = "https://sqs.us-east-2.amazonaws.com/848490464384/request.fifo";
string kCompactionResponseQueueUrl ="https://sqs.us-east-2.amazonaws.com/848490464384/response.fifo";

string waitForResponse(const string &queueUrl);
void sendMessage(const string &message, const string &queueUrl);

int main() {
  while (true) {
    string base64Input = waitForResponse(kCompactionRequestQueueUrl);
    cout << "received something" << endl;
    Aws::Utils::ByteBuffer inputBuffer = Aws::Utils::HashingUtils::Base64Decode(base64Input);
    std::string input(reinterpret_cast<char*>(inputBuffer.GetUnderlyingData()), inputBuffer.GetLength());

    if (input.empty()) {
      continue;
    }

    string output;
    CompactionServiceOptionsOverride options;
    options.env = rocksdb::Env::Default();
    // shouldn't be required
    // options.file_checksum_gen_factory = rocksdb::GetFileChecksumGenCrc32cFactory();
    // not really sure
    // options.comparator = rocksdb::BytewiseComparator();
    // pretty sure this is optional
    // options.merge_operator = rocksdb::GetAggMergeOperator();
    // pretty sure this is optional as well
    // options.compaction_filter = 
    // assuming this is optional as well
    // options.compaction_filter_factory = 
    // feature, likely optional
    // options.prefix_extractor = options_.prefix_extractor;
    options.table_factory = shared_ptr<rocksdb::TableFactory>(rocksdb::NewBlockBasedTableFactory());
    // options.sst_partitioner_factory = rocksdb::SstPartitionerFixedPrefixFactory();
    options.statistics = rocksdb::CreateDBStatistics();

    Status s = DB::OpenAndCompact(kDBPath, kDBCompactionOutputPath, input, &output, options);
    if (!s.ok()) {
      cout << "something bad happened" << endl;
      continue;
    }

    Aws::Utils::ByteBuffer outputBuffer(reinterpret_cast<const unsigned char*>(output.c_str()), output.length());
    std::string base64Output = Aws::Utils::HashingUtils::Base64Encode(outputBuffer);

    cout << "sending response" << endl;

    sendMessage(base64Output, kCompactionResponseQueueUrl);
  }

  return 0;
}

string waitForResponse(const string &queueUrl) {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  string result = "";
  {
    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = Aws::Region::US_EAST_2; // Set the region to Ohio

    Aws::SQS::SQSClient sqs(clientConfig);

    // Create a receive message request
    Aws::SQS::Model::ReceiveMessageRequest receive_request;
    receive_request.SetQueueUrl(queueUrl);
    receive_request.SetMaxNumberOfMessages(
        1); // Max number of messages to receive
    receive_request.SetVisibilityTimeout(30); // Visibility timeout
    receive_request.SetWaitTimeSeconds(20);   // Long polling wait time

    // Receive the message
    auto receive_outcome = sqs.ReceiveMessage(receive_request);

    if (receive_outcome.IsSuccess()) {
      const auto &messages = receive_outcome.GetResult().GetMessages();
      if (!messages.empty()) {
        for (const auto &message : messages) {
          result = message.GetBody();

          // After processing, delete the message from the queue
          Aws::SQS::Model::DeleteMessageRequest delete_request;
          delete_request.SetQueueUrl(queueUrl);
          delete_request.SetReceiptHandle(message.GetReceiptHandle());
          auto delete_outcome = sqs.DeleteMessage(delete_request);
          if (!delete_outcome.IsSuccess()) {
            std::cerr << "Error deleting message: "
                      << delete_outcome.GetError().GetMessage() << endl;
          }
        }
      } else {
        cout << "No messages to process." << endl;
      }
    } else {
      std::cerr << "Error receiving messages: "
                << receive_outcome.GetError().GetMessage() << endl;
    }
  }
  Aws::ShutdownAPI(options);
  return result;
}

void sendMessage(const string &message, const string &queueUrl) {
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  {
    auto now = std::chrono::high_resolution_clock::now();

    // Convert the time point to a duration since the epoch
    auto duration_since_epoch = now.time_since_epoch();

    // Convert the duration to a specific unit (e.g., nanoseconds)
    auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           duration_since_epoch)
                           .count();

    std::stringstream ss;
    ss << nanoseconds;
    string nanoStr = ss.str();

    // Hash the input string
    Aws::Utils::Crypto::Sha256 sha256;
    auto hashBytes = sha256.Calculate(message + nanoStr);
    auto hash = Aws::Utils::HashingUtils::HexEncode(hashBytes.GetResult());

    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = Aws::Region::US_EAST_2; // Set the region to Ohio

    Aws::SQS::SQSClient sqs(clientConfig);

    while (1) {
      Aws::SQS::Model::SendMessageRequest smReq;
      smReq.SetQueueUrl(queueUrl);
      smReq.SetMessageGroupId("group");
      smReq.SetMessageDeduplicationId(hash);
      smReq.SetMessageBody(message);

      auto sm_out = sqs.SendMessage(smReq);
      if (sm_out.IsSuccess()) {
        return;
      } else {
        std::cerr << "Error sending message: " << sm_out.GetError().GetMessage()
                  << endl;
      }
    }
  }
  Aws::ShutdownAPI(options);
}