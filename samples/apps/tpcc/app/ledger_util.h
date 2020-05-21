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
  char * buffer;
  size_t size;

public:
  Ledger(std::string ledger_path, uint64_t offset = 0)
  : ledger_path(ledger_path)
  , end_iter(nullptr)
  {
    std::ifstream fs(ledger_path);

    fs.seekg(offset);
    std::streampos offset_pos = fs.tellg();

    fs.seekg(0, fs.end);
    std::streampos end_pos = fs.tellg();

    size = end_pos - offset_pos;
    fs.seekg(offset);

    LOG_INFO_FMT("Reading ledger file, size: {}", size);

    buffer = new char[size];
    if (!fs.read(buffer, size))
    {
      LOG_INFO_FMT("Error: Could not read ledger file");
      throw std::logic_error("Ledger read failed");
    }
  }

  class iterator : public std::iterator<
                     std::input_iterator_tag,
                     LedgerDomain,
                     LedgerDomain,
                     const LedgerDomain*,
                     LedgerDomain&>
  {
  private:
    // File management
    char * buffer;
    size_t size;
    size_t iter_offset;

    // Transaction elements
    size_t txn_size;
    size_t txn_offset;
    size_t domain_size;
    size_t domain_offset;
    std::shared_ptr<LedgerDomain> domain_ptr;

    template <typename T>
    std::tuple<T> deserialize(char* buffer, const size_t size)
    {
      const uint8_t* data_ptr = (uint8_t*)buffer;
      size_t size_cpy = size;

      return serializer::CommonSerializer::deserialize<T>(data_ptr, size);
    }

    void read_header()
    {
      // Read transaction size field
      std::tuple<uint32_t> size_field = deserialize<uint32_t>(buffer + iter_offset, TXN_SIZE_FIELD);
      txn_size = std::get<0>(size_field);

      // Update offset for start of transaction data
      txn_offset = iter_offset + TXN_SIZE_FIELD;

      // Update offset for next iteration
      iter_offset += (txn_size + TXN_SIZE_FIELD);

      // Offset into the header
      size_t header_offset = 0;

      // Read AES GCM header (seek past)
      header_offset += GCM_SIZE_FIELD;

      // Read public domain header
      std::tuple<uint64_t> domain_size_field = deserialize<uint64_t>(
        buffer + txn_offset + header_offset, DOMAIN_SIZE_FIELD);
      header_offset += DOMAIN_SIZE_FIELD;

      // Get the size of the public domain
      domain_size = std::get<0>(domain_size_field);

      // Update offset for the start of the public domain
      domain_offset = txn_offset + header_offset;
    }

  public:
    iterator(char * buffer, size_t size, bool seek_end = false) :
      buffer(buffer),
      size(size),
      iter_offset(seek_end ? size : 0),
      txn_size(0),
      txn_offset(0),
      domain_size(0),
      domain_offset(0),
      domain_ptr(nullptr)
    {
      if (!seek_end)
        read_header();
    }

    iterator& operator++()
    {
      if (iter_offset >= size)
      {
        return *this;
      }

      // Reset stored transaction data
      domain_ptr.reset();

      // Reset offsets and sizes
      txn_size = 0;
      txn_offset = 0;
      domain_size = 0;
      domain_offset = 0;

      // Read next header
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

    LedgerDomain& operator*()
    {
      if (domain_ptr == nullptr)
      {
        domain_ptr = std::make_shared<LedgerDomain>(buffer + domain_offset, domain_size);
      }

      return *domain_ptr;
    }

    std::tuple<uint8_t*, size_t> get_raw_data()
    {
      return std::make_tuple((uint8_t*) buffer + txn_offset, txn_size);
    }
  };

  std::shared_ptr<iterator> end_iter;

  iterator begin()
  {
    return iterator(buffer, size);
  }

  iterator end()
  {
    if (end_iter == nullptr)
      end_iter = std::make_shared<iterator>(buffer, size, true);

    return *end_iter;
  }
};