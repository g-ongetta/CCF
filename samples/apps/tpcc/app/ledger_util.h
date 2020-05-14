// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#pragma once

#include "ds/serializer.h"

#include <fstream>
#include <memory>
#include <msgpack/msgpack.hpp>
#include <string>

namespace
{
  // Sizes (in bytes) of particular fields in the transaction header
  static const size_t TXN_SIZE_FIELD = 4;
  static const size_t DOMAIN_SIZE_FIELD = 8;
  static const size_t GCM_SIZE_TAG = 16;
  static const size_t GCM_SIZE_IV = 12;
  static const size_t GCM_SIZE_FIELD = GCM_SIZE_TAG + GCM_SIZE_IV;
}

class LedgerDomain
{
private:
  char* buffer;
  size_t length;
  size_t offset;

  uint64_t version;

  struct kv_update
  {
    msgpack::unpacked key;
    msgpack::unpacked val;
  };

  std::map<std::string, std::vector<kv_update>> table_updates;
  std::vector<std::string> table_names;

  msgpack::unpacked unpack()
  {
    msgpack::unpacked result;
    msgpack::unpack(result, buffer, length, offset);
    return result;
  }

public:
  LedgerDomain(char* buffer, size_t length) :
    buffer(buffer),
    length(length),
    offset(0),
    version(),
    table_names(),
    table_updates()
  {
    // Read version
    version = unpack().get().convert();

    // Read tables
    while (offset != length)
    {
      // Unpack table (map) metadata
      msgpack::unpacked map_start_indicator = unpack();
      msgpack::unpacked map_name = unpack();
      std::string map_name_str = map_name.get().convert();

      table_names.push_back(map_name_str);

      msgpack::unpacked read_version = unpack();
      msgpack::unpacked read_count = unpack();

      // Unpack table writes
      size_t write_count = unpack().get().convert();

      std::vector<kv_update> updates;
      updates.reserve(write_count);

      for (auto i = 0; i < write_count; i++)
      {
        kv_update update = {unpack(), unpack()};
        updates.push_back(std::move(update));
      }

      // Unpack table removes
      msgpack::unpacked remove_count = unpack();
      size_t remove_count_num = remove_count.get().convert();
      for (auto i = 0; i < remove_count_num; i++)
      {
        msgpack::unpacked key = unpack();
        // table.erase(key);
        // TODO!
      }

      table_updates.insert(std::make_pair(map_name_str, std::move(updates)));
    }
  }

  uint64_t get_version() const
  {
    return version;
  }

  std::vector<std::string> get_table_names() const
  {
    return table_names;
  }

  template <typename K, typename V>
  std::map<K, V> get_table_updates(std::string table_name) const
  {
    auto iter = table_updates.find(table_name);
    if (iter == table_updates.end())
    {
      return {};
    }

    std::map<K, V> updates;
    for (auto& update : iter->second)
    {
      msgpack::object key_obj = update.key.get();
      msgpack::object val_obj = update.val.get();
      updates.emplace(key_obj.as<K>(), val_obj.as<V>());
    }

    return updates;
  }
};

class Ledger
{
private:
  const std::string ledger_path;

public:
  Ledger(std::string ledger_path) : ledger_path(ledger_path) {}

  class iterator : public std::iterator<
                     std::input_iterator_tag,
                     LedgerDomain,
                     LedgerDomain,
                     const LedgerDomain*,
                     LedgerDomain&>
  {
  private:
    // File management
    std::ifstream fs;
    size_t file_size;
    size_t iter_offset;
    bool completed_read;

    // Transaction elements
    size_t txn_size;
    size_t domain_size;
    size_t domain_offset;
    std::shared_ptr<LedgerDomain> domain_ptr;

    // Raw transaction data
    char* data_buffer;
    size_t data_offset;

    template <typename T>
    std::tuple<T> deserialize(char* buffer, const size_t size)
    {
      const uint8_t* data_ptr = (uint8_t*)buffer;
      size_t size_cpy = size;

      return serializer::CommonSerializer::deserialize<T>(data_ptr, size);
    }

    void read_ledger_file(size_t size)
    {
      if (!fs.read(data_buffer + data_offset, size))
      {
        std::string err_msg = "Ledger Read Failed";
        LOG_INFO_FMT(err_msg);
        throw std::logic_error(err_msg);
      }

      data_offset += size;
    }

    void read_header()
    {
      // Read transaction size field
      char* txn_size_buffer = new char[TXN_SIZE_FIELD];
      if (!fs.read(txn_size_buffer, TXN_SIZE_FIELD))
      {
        LOG_INFO_FMT(
          "Ledger Read Error: Could not read transaction size field");
        throw std::logic_error("Ledger Read Failed");
      }

      // Deserialize transaction size
      std::tuple<uint32_t> size_field =
        deserialize<uint32_t>(txn_size_buffer, TXN_SIZE_FIELD);
      txn_size = std::get<0>(size_field);

      // Create buffer to store raw transaction data
      data_buffer = new char[txn_size];

      delete[] txn_size_buffer;

      // Update iterator offset
      iter_offset += (txn_size + TXN_SIZE_FIELD);

      // Read AES GCM header
      read_ledger_file(GCM_SIZE_FIELD);

      // Read public domain header
      read_ledger_file(DOMAIN_SIZE_FIELD);

      // Deserialise public domain header
      std::tuple<uint64_t> domain_size_field = deserialize<uint64_t>(
        data_buffer + data_offset - DOMAIN_SIZE_FIELD, DOMAIN_SIZE_FIELD);
      domain_size = std::get<0>(domain_size_field);

      domain_offset = data_offset;
    }

  public:
    iterator(std::string ledger_path, uint64_t offset) :
      fs(),
      file_size(0),
      iter_offset(0),
      completed_read(false),
      txn_size(0),
      domain_size(0),
      domain_offset(0),
      domain_ptr(nullptr),
      data_buffer(nullptr),
      data_offset(0)
    {
      fs.open(ledger_path, std::ifstream::binary);

      // Find the file length
      fs.seekg(0, fs.end);
      file_size = fs.tellg();
      iter_offset += file_size;

      // If offset -1, leave iter at end of file
      if (offset == -1)
        return;

      fs.seekg(offset, fs.beg);
      iter_offset = offset;

      LOG_DEBUG_FMT("Ledger file size: {}", file_size);
      read_header();
    }

    ~iterator()
    {
      fs.close();
    }

    iterator& operator++()
    {
      if (iter_offset >= file_size)
      {
        return *this;
      }

      // Reset stored transaction data
      domain_ptr.reset();
      completed_read = false;

      txn_size = 0;
      data_offset = 0;
      domain_offset = 0;
      delete[] data_buffer;

      // Read next header
      fs.seekg(iter_offset);
      read_header();

      return *this;
    }

    bool operator==(iterator other) const
    {
      return iter_offset == other.iter_offset;
    }

    bool operator<=(iterator other) const
    {
      return iter_offset <= other.iter_offset;
    }

    bool operator<(iterator other) const
    {
      return iter_offset < other.iter_offset;
    }

    void read_domain()
    {
      if (completed_read)
        return;

      // Read the public domain
      read_ledger_file(domain_size);

      // Check if further data exists for this tx beyond public domain
      uint64_t bytes_remaining = txn_size - data_offset;
      if (bytes_remaining > 0)
      {
        read_ledger_file(bytes_remaining);
      }

      completed_read = true;
    }

    LedgerDomain& operator*()
    {
      if (!completed_read)
        read_domain();

      if (domain_ptr == nullptr)
        domain_ptr = std::make_shared<LedgerDomain>(
          data_buffer + domain_offset, domain_size);

      return *domain_ptr;
    }

    std::tuple<uint8_t*, size_t> get_raw_data()
    {
      if (!completed_read)
        read_domain();

      return std::make_tuple((uint8_t*)data_buffer, txn_size);
    }
  };

  iterator begin(uint64_t offset)
  {
    return iterator(ledger_path, offset);
  }

  iterator begin()
  {
    return iterator(ledger_path, 0);
  }

  iterator end()
  {
    return iterator(ledger_path, -1);
  }
};