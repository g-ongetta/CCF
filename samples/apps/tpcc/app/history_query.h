// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#pragma once

#include "ledger_reader.h"
#include "snapshot_reader.h"
#include "ledger_util.h"
#include "kv/tpcc_entities.h"

#include <chrono>

using HistoryId = ccfapp::tpcc::HistoryId;
using History = ccfapp::tpcc::History;
using HistoryView = ccf::Store::Map<HistoryId, History>::TxView;

class HistoryQuery
{
private:
  TimePoint date_from;
  TimePoint date_to;

  HistoryView* history_view;

  TimePoint parse_time(std::string time_string)
  {
    std::tm date_tm = {};
    std::istringstream ss_to(time_string);
    ss_to >> std::get_time(&date_tm, "%F %T");

    return std::chrono::system_clock::from_time_t(mktime(&date_tm));
  }

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

      // Parse History date and convert date to time_point
      TimePoint date = parse_time(val.date);

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
      // Convert date string to time point for comparison
      TimePoint date = parse_time(val.date);

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

  void query_snapshots(kv::SnapshotManager snapshot_manager, ccf::Nodes::TxView* nodes_view, std::vector<uint64_t>& results)
  {
    LOG_INFO << "Processing Snapshot query..." << std::endl;

    // std::vector<kv::Snapshot> snapshots = snapshot_manager.get_snapshots();

    // kv::Snapshot start;

    // for (int i = 0; i < snapshots.size(); i++)
    // {
    //   kv::Snapshot snapshot = snapshots[i];
    //   TimePoint date = snapshot.get_index_value();

    //   if (date > date_from)
    //   {
    //     if (i == 0)
    //     {
    //       LOG_INFO_FMT("Error: Query range preceeds snapshots");
    //       throw std::logic_error("Snapshot query error");
    //     }

    //     start = snapshots[i - 1];
    //     break;
    //   }
    // }

    goodliffe::multi_skip_list<kv::Snapshot> snapshots = snapshot_manager.get_snapshots();

    kv::Snapshot comparator;
    comparator.set_index_value(date_from);
    auto snapshots_iter = snapshots.lower_bound(comparator);

    kv::Snapshot start = *(--snapshots_iter);

    LOG_INFO_FMT("Query starts at snapshot {}", start.get_version());

    // First check snapshot
    SnapshotReader snapshot_reader(start);
    std::vector<std::string> snapshot_tables = snapshot_reader.read();

    // If snapshot contains history entries, add them to results
    if (std::find(snapshot_tables.begin(), snapshot_tables.end(), "histories") != snapshot_tables.end())
    {
      auto table_snapshot = snapshot_reader.get_table_snapshot<HistoryId, History>("histories");
      std::map<HistoryId, History> history_table = table_snapshot->get_table();

      for (auto iter = history_table.begin(); iter != history_table.end(); ++iter)
      {
        History history = iter->second;
        TimePoint history_date = parse_time(history.date);

        if (history_date >= date_from && history_date <= date_to)
          results.push_back(iter->first);
      }
    }

    // Second, replay ledger from snapshot until range is exceeded
    std::string ledger_path = "0.ledger";
    LedgerReader ledger_reader(ledger_path, nodes_view, start.get_ledger_offset(), start.get_merkle_root());

    while (ledger_reader.has_next())
    {
      auto batch = ledger_reader.read_batch();
      if (batch == nullptr)
      {
        LOG_INFO_FMT("Ledger batch was null");
        throw std::logic_error("Ledger read error");
      }

      for (auto& domain : *batch)
      {
        bool exeeded_range = process_domain(domain, results);

        if (exeeded_range)
          return;
      }
    }
  }
};