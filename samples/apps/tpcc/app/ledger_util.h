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

  msgpack::zone zone;

  std::map<std::string, std::vector<kv_update>> table_updates;
  std::vector<std::string> table_names;

  msgpack::object unpack()
  {
    zone.clear();
    return msgpack::unpack(zone, buffer, length, offset);
  }

  msgpack::unpacked unpack_persistent()
  {
    msgpack::unpacked result;
    msgpack::unpack(result, buffer, length, offset);
    return result;
  }

public:
  LedgerDomain(char* buffer, size_t length, std::vector<std::string> table_names) :
    buffer(buffer),
    length(length),
    table_names(table_names),
    offset(0),
    zone(),
    version(),
    table_updates()
  {
    // Read version
    version = unpack().convert();

    // Read tables
    while (offset != length)
    {
      unpack(); // Map start indicator

      std::string map_name_str = unpack().convert();

      auto iter = std::find(table_names.begin(), table_names.end(), map_name_str);
      bool persist_data = iter != table_names.end();

      unpack(); // Read version
      unpack(); // Read count

      // If persist data, save objects to be retrieved, otherwise discard
      if (persist_data)
      {
        size_t write_count = unpack().convert();

        std::vector<kv_update> updates;
        updates.reserve(write_count);

        for (auto i = 0; i < write_count; i++)
        {
          kv_update update = {unpack_persistent(), unpack_persistent()};
          updates.push_back(std::move(update));
        }

        // Unpack table removes
        size_t remove_count_num = unpack().convert();
        for (auto i = 0; i < remove_count_num; i++)
        {
          msgpack::unpacked key = unpack_persistent();
          // table.erase(key);
          // TODO!
        }

        table_updates.insert(std::make_pair(map_name_str, std::move(updates)));
      }
      else
      {
        size_t write_count = unpack().convert();
        for (auto i = 0; i < write_count; i++)
        {
          unpack(); // Key
          unpack(); // Data
        }

        size_t remove_count = unpack().convert();
        for (auto i = 0; i < remove_count; i++)
        {
          unpack(); // Key
        }
      }
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
  size_t offset;

  std::vector<std::string> tables;

public:
  Ledger(std::string ledger_path, std::vector<std::string> tables, uint64_t offset = 0)
  : ledger_path(ledger_path)
  , tables(tables)
  , end_iter(nullptr)
  , offset(offset)
  , buffer()
  , size()
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

  ~Ledger()
  {
    delete[] buffer;
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

    // Tables for which data will be retrieved upon read
    std::vector<std::string> tables;

    template <typename T>
    std::tuple<T> deserialize(char* data_buffer, const size_t size)
    {
      const uint8_t* data_ptr = (uint8_t*)data_buffer;
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
      // LOG_INFO_FMT("finished reading header");
    }

  public:
    iterator(char * buffer, size_t size, std::vector<std::string> tables, bool seek_end = false) :
      buffer(buffer),
      size(size),
      tables(tables),
      iter_offset(seek_end ? size : 0),
      txn_size(0),
      txn_offset(0),
      domain_size(0),
      domain_offset(0),
      domain_ptr(nullptr)
    {
      if (iter_offset < size)
        read_header();
    }

    uint64_t get_iter_offset()
    {
      return iter_offset;
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
        domain_ptr = std::make_shared<LedgerDomain>(buffer + domain_offset, domain_size, tables);
      }

      return *domain_ptr;
    }

    std::tuple<char *, size_t> get_raw_data()
    {
      return std::make_tuple(buffer + txn_offset, txn_size);
    }
  };

  std::shared_ptr<iterator> end_iter;

  iterator begin()
  {
    return iterator(buffer, size, tables);
  }

  iterator end()
  {
    if (end_iter == nullptr)
      end_iter = std::make_shared<iterator>(buffer, size, tables, true);

    return *end_iter;
  }
};