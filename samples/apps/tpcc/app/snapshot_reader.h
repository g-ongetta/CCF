// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#pragma once

#include "consensus/pbft/libbyz/digest.h"
#include "ds/serializer.h"

#include <msgpack/msgpack.hpp>

using namespace ccf;

template <typename K, typename V>
class TableSnapshot
{
private:
  char* buffer;
  size_t length;
  size_t offset;

  std::string table_name;
  std::map<K, V> table;

  msgpack::unpacked unpack()
  {
    msgpack::unpacked result;
    msgpack::unpack(result, buffer, length, offset);
    return result;
  }

public:
  TableSnapshot(char* buffer, size_t length, std::string name) :
    buffer(buffer), length(length), offset(0), table_name(name)
  {
    while (offset != length)
    {
      msgpack::unpacked key_obj = unpack();
      msgpack::unpacked val_obj = unpack();

      K key = key_obj.get().convert();
      V val = val_obj.get().convert();

      table.emplace(key, val);
    }
  }

  std::string get_name()
  {
    return table_name;
  }

  std::map<K, V> get_table()
  {
    return table;
  }
};



class SnapshotReader
{
private:
  std::ifstream fs;
  size_t file_size;
  size_t offset;

  uint64_t version;
  bool is_read;

  std::unordered_map<std::string, std::tuple<char *, size_t>> table_buffers;
  Snapshots::TxView* snapshots_view;

  void verify_hash(char * hash_bytes)
  {
    // Find signed hash from the key value store for verification
    auto signed_hash = snapshots_view->get(version);
    if (!signed_hash.has_value())
    {
      LOG_INFO_FMT("Error: Could not find snapshot signature {} from KV", version);
      throw std::logic_error("Snapshot verification failed");
    }

    std::vector<uint8_t> signed_hash_bytes = signed_hash.value();

    // Verify that the hashes are equal
    for (int i = 0; i < signed_hash_bytes.size(); i++)
    {
      if (signed_hash_bytes[i] != (uint8_t) hash_bytes[i])
      {
        LOG_INFO_FMT("Error: Snapshot hash does not equal signed hash");
        throw std::logic_error("Snapshot verification failed");
      }
    }
  }

public:
  SnapshotReader(uint64_t version, Snapshots::TxView* snapshots_view)
  : version(version)
  , fs()
  , file_size()
  , table_buffers()
  , is_read(false)
  , snapshots_view(snapshots_view)
  {
    fs.open(fmt::format("snapshot_v{}", version), std::ifstream::binary);
    fs.seekg(0, fs.end);
    file_size = fs.tellg();
    fs.seekg(0, fs.beg);
  }

  /*
    Reads and verifies snapshot file.
    @throw std::logic error if verification fails
    @return vector of tables found in snapshot
  */
  std::vector<std::string> read()
  {
    if (is_read)
      return {};

    LOG_INFO_FMT("Reading snapshot ver.{}", version);

    std::vector<std::string> table_names;

    Digest::Context context;
    Digest digest;

    size_t offset = 0;

    while (offset < file_size)
    {
      static const int HEADER_SIZE_FIELD = 2;

      // Read header size from file
      size_t header_size;
      fs >> header_size;

      // Read header from file
      char * header = new char[header_size];
      if (!fs.read(header, header_size))
      {
        LOG_INFO_FMT("Snapshot Error: could not read header");
        throw std::logic_error("Snapshot read failed");
      }

      // Unpack header
      size_t unpack_offset = 0;
      msgpack::unpacked table_name_obj;
      msgpack::unpacked table_size_obj;

      msgpack::unpack(table_name_obj, header, header_size, unpack_offset);
      msgpack::unpack(table_size_obj, header, header_size, unpack_offset);

      std::string table_name = table_name_obj.get().convert();
      size_t data_size = table_size_obj.get().convert();

      offset += (data_size + header_size + HEADER_SIZE_FIELD);

      table_names.push_back(table_name);

      // Read table data from file
      char * data = new char[data_size];
      if (!fs.read(data, data_size))
      {
        LOG_INFO_FMT("Error: Could not read table data");
        throw std::logic_error("Snapshot read failed");
      }

      // Update hash digest
      digest.update_last(context, header, header_size);
      digest.update_last(context, data, data_size);

      table_buffers.emplace(table_name, std::make_tuple(data, data_size));
    }

    digest.finalize(context);
    verify_hash(digest.digest());

    is_read = true;
    return table_names;
  }

  template <typename K, typename V>
  std::shared_ptr<TableSnapshot<K, V>> get_table_snapshot(std::string table)
  {
    if (!is_read)
      return nullptr;

    auto iter = table_buffers.find(table);
    if (iter == table_buffers.end())
    {
      LOG_INFO_FMT("Error: Could not find table in snapshot buffers");
      throw std::logic_error("Snapshot read error");
    }

    auto [buffer, size] = iter->second;
    return std::make_shared<TableSnapshot<K, V>>(buffer, size, table);
  }

};
