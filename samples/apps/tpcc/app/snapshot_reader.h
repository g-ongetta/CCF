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
  kv::Snapshot snapshot;

  std::ifstream fs;
  size_t file_size;
  size_t offset;

  bool is_read;

  std::unordered_map<std::string, std::tuple<char *, size_t>> table_buffers;

  void verify_hash(char * hash_bytes)
  {
    std::vector<uint8_t> signed_hash_bytes = snapshot.get_hash();

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
  SnapshotReader(kv::Snapshot snapshot)
  : snapshot(snapshot)
  , fs()
  , file_size()
  , table_buffers()
  , is_read(false)
  {
    fs.open(fmt::format("snapshot_v{}", snapshot.get_version()), std::ifstream::binary);
    fs.seekg(0, fs.end);
    file_size = fs.tellg();
    fs.seekg(0, fs.beg);
  }

  ~SnapshotReader()
  {
    LOG_INFO_FMT("Deleting snapshot reader buffers");
    for (auto iter = table_buffers.begin(); iter != table_buffers.end(); ++iter)
    {
      char * buffer = std::get<0>(iter->second);
      delete[] buffer;
    }
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

    LOG_INFO_FMT("Reading snapshot v.{}, size: {}", snapshot.get_version(), file_size);

    std::vector<std::string> table_names;

    Digest::Context context;
    Digest digest;

    char * buffer = new char[file_size];
    if (!fs.read(buffer, file_size))
    {
      LOG_INFO_FMT("Snapshot Error: could not read snapshot file");
      throw std::logic_error("Snapshot read failed");
    }

    size_t offset = 0;

    while (offset < file_size)
    {
      size_t header_offset = offset;

      // Unpack header
      msgpack::unpacked table_name_obj;
      msgpack::unpacked table_size_obj;
      msgpack::unpack(table_name_obj, buffer, file_size, offset);
      msgpack::unpack(table_size_obj, buffer, file_size, offset);

      size_t data_offset = offset;

      std::string table_name = table_name_obj.get().convert();
      size_t data_size = table_size_obj.get().convert();
      table_names.push_back(table_name);

      // Update hash digest
      digest.update_last(context, buffer + header_offset, data_offset - header_offset);
      digest.update_last(context, buffer + data_offset, data_size);

      char * data = new char[data_size];
      memcpy(data, buffer + data_offset, data_size);

      table_buffers.emplace(table_name, std::make_tuple(data, data_size));

      offset += data_size;
    }

    delete[] buffer;

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
      LOG_INFO_FMT("Error: Could not find table {} in snapshot buffers", table);
      throw std::logic_error("Snapshot read error");
    }

    auto [buffer, size] = iter->second;
    return std::make_shared<TableSnapshot<K, V>>(buffer, size, table);
  }

};
