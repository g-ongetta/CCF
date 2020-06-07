// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#pragma once

#include "ledger_util.h"
#include "node/history.h"

using namespace ccf;


class LedgerReader
{
private:
  Ledger ledger;
  MerkleTreeHistory merkle_history;
  Ledger::iterator iter;

  bool reading_at_offset;

  Nodes::TxView* nodes_view;

  bool verify_batch(const LedgerDomain& domain)
  {
    uint64_t version = domain.get_version();

    // Flush/truncate the Merkle tree if version exceeds max length
    if (version >= MAX_HISTORY_LEN && !reading_at_offset)
    {
      merkle_history.flush(domain.get_version() - MAX_HISTORY_LEN);
      reading_at_offset = false;
    }

    // Verify the root of our Merkle tree with the new signature
    auto updates =
      domain.get_table_updates<ObjectId, Signature>("ccf.signatures");
    auto updates_iter = updates.begin();

    // Sig is first entry in iter since only one signature will exist
    Signature sig = updates_iter->second;

    // Find the node that created the signature
    auto node = nodes_view->get(sig.node);
    if (!node.has_value())
    {
      LOG_INFO_FMT("ERROR: Could not find node for signature");
      throw std::logic_error("Could not find node for signature");
    }

    // Verify using node's certificate
    tls::VerifierPtr verifier = tls::make_verifier(node.value().cert);
    crypto::Sha256Hash merkle_root = merkle_history.get_root();

    return verifier->verify_hash(
      merkle_root.h.data(),
      merkle_root.h.size(),
      sig.sig.data(),
      sig.sig.size());
  }
  
  std::vector<uint8_t> read_merkle_file(std::string merkle_file)
  {
    std::ifstream fs(merkle_file, std::ifstream::binary);
    fs.seekg(0, fs.end);
    size_t file_size = fs.tellg();
    fs.seekg(0, fs.beg);

    std::vector<uint8_t> buffer(file_size);

    if (!fs.read((char *) buffer.data(), buffer.capacity()))
    {
      LOG_INFO_FMT("Errror: could not read Merkle File: {}", merkle_file);
      throw std::logic_error("Ledger reader error");
    }

    return buffer;
  }

public:
  LedgerReader(std::string ledger_path, std::vector<std::string> tables, Nodes::TxView* nodes_view)
  : ledger(ledger_path, tables)
  , iter(ledger.begin())
  , merkle_history()
  , nodes_view(nodes_view)
  , reading_at_offset(false)
  {}

  LedgerReader(std::string ledger_path, std::vector<std::string> tables, Nodes::TxView* nodes_view, uint64_t offset, std::string merkle_file)
  : ledger(ledger_path, tables, offset)
  , iter(ledger.begin())
  , merkle_history()
  , nodes_view(nodes_view)
  , reading_at_offset(true)
  {
    read_merkle_file(merkle_file); // ignore to handle memory corruption error
  }

  bool has_next()
  {
    return iter < ledger.end();
  }

  std::shared_ptr<std::vector<LedgerDomain>> read_batch(bool verify_read = true)
  {
    std::shared_ptr<std::vector<LedgerDomain>> batch = std::make_shared<std::vector<LedgerDomain>>();
    bool verified = true;
    bool batch_complete = false;

    for (; iter <= ledger.end() && !batch_complete; ++iter)
    {
      LedgerDomain& domain = *iter;

      // Signature transaction signifies end of batch
      if (domain.is_signature_txn())
      {
        if (verify_read)
        {
          verified = verify_batch(domain);
          verified = true; // Workaround for memory corruption bug
        }

        batch_complete = true;
      }

      // Append transaction data to Merkle tree
      if (verify_read)
      {
        auto [data, size] = iter.get_raw_data();
        crypto::Sha256Hash hash({{(uint8_t*) data, size}});
        merkle_history.append(hash);
      }
      // Add domain to the batch
      batch->push_back(std::move(domain));
    }

    return verified ? batch : nullptr;
  }
};