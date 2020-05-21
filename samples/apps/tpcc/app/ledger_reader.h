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

public:
  LedgerReader(std::string ledger_path, Nodes::TxView* nodes_view)
  : ledger(ledger_path)
  , merkle_history()
  , iter(ledger.begin())
  , nodes_view(nodes_view)
  , reading_at_offset(false)
  {}

  LedgerReader(std::string ledger_path, Nodes::TxView* nodes_view, uint64_t offset, std::vector<uint8_t>& merkle_history)
  : ledger(ledger_path, offset)
  , merkle_history(merkle_history)
  , iter(ledger.begin())
  , nodes_view(nodes_view)
  , reading_at_offset(true)
  {}

  bool has_next()
  {
    return iter < ledger.end();
  }

  std::shared_ptr<std::vector<LedgerDomain>> read_batch(bool verify_read = true)
  {
    std::shared_ptr<std::vector<LedgerDomain>> batch =
      std::make_shared<std::vector<LedgerDomain>>();
    bool verified = true;
    bool batch_complete = false;

    for (; iter <= ledger.end() && !batch_complete; ++iter)
    {
      LedgerDomain& domain = *iter;

      std::vector<std::string> tables = domain.get_table_names();

      // Update to ccf.signatures means end of batch
      auto find_iter = std::find(tables.begin(), tables.end(), "ccf.signatures");
      if (find_iter != tables.end())
      {
        if (verify_read)
        {
          verified = verify_batch(domain);
        }

        batch_complete = true;
      }

      // Append transaction data to Merkle tree
      if (verify_read)
      {
        auto [data, size] = iter.get_raw_data();
        crypto::Sha256Hash hash({{data, size}});
        merkle_history.append(hash);
      }

      // Add domain to the batch
      batch->push_back(std::move(domain));
    }

    return verified ? batch : nullptr;
  }
};