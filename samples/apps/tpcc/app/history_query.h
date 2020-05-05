// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#pragma once

#include "ledger_reader.h"
#include "ledger_util.h"
#include "tpcc_entities.h"

#include <chrono>

using TimePoint = std::chrono::system_clock::time_point;

using HistoryId = ccfapp::tpcc::HistoryId;
using History = ccfapp::tpcc::History;
using HistoryView = ccf::Store::Map<HistoryId, History>::TxView;

class HistoryQuery
{
private:
  TimePoint date_from;
  TimePoint date_to;

  HistoryView* history_view;

  /*
    Processes a LedgerDomain object for a ledger query.
    @return: true if the query range was exceeded, false otherwise
  */
  bool process_domain(LedgerDomain& domain, std::vector<uint64_t>& results)
  {
    std::vector<std::string> tables = domain.get_table_names();

    // Continue if no history updates in current transaction
    if (std::find(tables.begin(), tables.end(), "histories") == tables.end())
      return false;

    auto updates = domain.get_table_updates<HistoryId, History>("histories");

    for (auto updates_iter = updates.begin(); updates_iter != updates.end();
          ++updates_iter)
    {
      HistoryId key = updates_iter->first;
      History val = updates_iter->second;

      // Parse date of History entry
      std::tm date_tm = {};
      std::istringstream ss_to(val.date);
      ss_to >> std::get_time(&date_tm, "%F %T");

      // Convert date to time_point for comparison
      TimePoint date =
        std::chrono::system_clock::from_time_t(mktime(&date_tm));

      // Check if date of History entry lies within 'from' range
      if (date >= date_from)
      {
        // If date also within 'to' range, add to results, else stop search
        if (date <= date_to)
        {
          results.push_back(val.c_id);
        }
        else
        {
          // Exceeded range, return true
          return true;
        }
      }
    }

    return false;
  }

public:
  HistoryQuery(TimePoint from, TimePoint to, HistoryView* view) :
    date_from(from), date_to(to), history_view(view)
  {}

  void query_kv(std::vector<uint64_t>& results)
  {
    LOG_INFO << "Processing History Query via KV Store" << std::endl;

    history_view->foreach([&](const auto& key, const auto& val) {
      // Parse date of History entry
      std::tm date_tm = {};
      std::istringstream ss_to(val.date);
      ss_to >> std::get_time(&date_tm, "%F %T");

      // Convert date to time point for comparison
      TimePoint date = std::chrono::system_clock::from_time_t(mktime(&date_tm));

      // Check if date of History entry lies within range, if so add to results
      if (date <= date_to && date >= date_from)
      {
        results.push_back(val.c_id);
      }

      return true;
    });
  }

  void query_ledger(std::vector<uint64_t>& results)
  {
    LOG_INFO << "Processing History Query via Ledger Replay" << std::endl;

    std::string path = "0.ledger";
    Ledger ledger_reader(path);

    // Tracks when last update is found
    bool exceeded_range = false;

    // Start querying from beginning of ledger, until last update is found that
    // satisfies
    for (auto iter = ledger_reader.begin(); iter <= ledger_reader.end(); ++iter)
    {
      LedgerDomain& domain = *iter;

      if (process_domain(domain, results))
        break;
    }
  }

  void query_ledger_verified(
    ccf::Nodes::TxView* nodes_view, std::vector<uint64_t>& results)
  {

    LOG_INFO << "Processing History Query via Verified Ledger Replay"
             << std::endl;

    std::string path = "0.ledger";
    LedgerReader reader(path, nodes_view);

    bool exceeded_range = false;

    while (reader.has_next())
    {
      auto batch = reader.read_batch();
      if (batch == nullptr)
      {
        throw std::logic_error("Ledger Read Failed: Could not verify batch");
      }

      for (auto& domain : *batch)
      {
        if (process_domain(domain, results))
        {
          exceeded_range = true;
          break;
        }
      }
    }
  }

  void query_snapshots(std::vector<uint64_t>& results)
  {
    LOG_INFO << "Processing Snapshot query..." << std::endl;

  }
};