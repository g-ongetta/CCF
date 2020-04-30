// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#pragma once

#include "consensus/pbft/libbyz/digest.h"
#include "ds/serializer.h"
#include "tpcc_entities.h"

#include <msgpack/msgpack.hpp>

using namespace ccf;

class Snapshot
{
private:
  std::ofstream fs;
  Store::Tx& tx;

  bool finalized;

  // Hash digest
  Digest::Context context;
  Digest digest;

  uint32_t buf_size(std::stringstream& buf)
  {
    buf.seekg(0, buf.end);
    int size = buf.tellg();
    buf.seekg(0, buf.beg);

    return size;
  }

public:
  Snapshot(std::string path, Store::Tx& tx) :
    fs(path, std::ofstream::out), tx(tx), finalized(false), context(), digest()
  {}

  template <typename K, typename V>
  void serialize_table(Store::Map<K, V>& table, std::string name)
  {
    if (finalized)
    {
      throw std::logic_error("Serialize Error: Snapshot has been completed");
    }

    auto view = tx.get_view(table);

    msgpack::sbuffer data_buf;

    view->foreach([&](const auto& key, const auto& val) {
      msgpack::zone key_zone;
      msgpack::zone val_zone;

      msgpack::object key_obj(key, key_zone);
      msgpack::object val_obj(val, val_zone);

      msgpack::pack(data_buf, key_obj);
      msgpack::pack(data_buf, val_obj);

      return true;
    });

    // Buffer for header data
    msgpack::sbuffer header_buf;
    msgpack::pack(header_buf, name);
    msgpack::pack(header_buf, data_buf.size());

    size_t header_size = header_buf.size();
    size_t data_size = data_buf.size();

    // Add data to hash digest
    digest.update_last(context, header_buf.data(), header_size);
    digest.update_last(context, data_buf.data(), data_size);

    // Write header size to file
    fs << header_size;

    // Write header and check result
    if (!fs.write(header_buf.data(), header_size))
    {
      LOG_INFO_FMT("Snapshot Error: Could not write header");
      throw std::logic_error("Snapshot creation error");
    }

    // Write data and check result
    if (!fs.write(data_buf.data(), data_size))
    {
      LOG_INFO_FMT("Snapshot error: Could not write data");
      throw std::logic_error("Snapshot creation error");
    }
  }

  void finalize()
  {
    fs.close();
    digest.finalize(context);

    finalized = true;
  }

  std::vector<uint8_t> hash()
  {
    if (!finalized)
    {
      throw std::logic_error("Cannot get hash of non-finalized snapshot");
    }

    std::vector<uint8_t> hash_bytes;

    char* hash = digest.digest();
    for (int i = 0; i < 32; i++)
    {
      hash_bytes.push_back(hash[i]);
    }

    return hash_bytes;
  }
};

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
  std::string file_path;

public:
  SnapshotReader(const std::string& file_path) : file_path(file_path) {}

  class iterator : public std::iterator<
                     std::input_iterator_tag,
                     std::string,
                     std::string,
                     const std::string*,
                     std::string&>
  {
  private:
    std::ifstream fs;
    size_t file_size;
    size_t iter_offset;

    std::string table_name;
    size_t table_size;

    void read_table_header()
    {
      static const int HEADER_SIZE_FIELD = 2;

      // Read header size from file
      size_t header_size;
      fs >> header_size;

      // Read header from file
      char* header_buf = new char[header_size];
      if (!fs.read(header_buf, header_size))
      {
        LOG_INFO_FMT("Snapshot Error: could not read header");
        throw std::logic_error("Snapshot read failed");
      }

      size_t unpack_offset = 0;
      msgpack::unpacked table_name_obj;
      msgpack::unpacked table_size_obj;

      msgpack::unpack(table_name_obj, header_buf, header_size, unpack_offset);
      msgpack::unpack(table_size_obj, header_buf, header_size, unpack_offset);

      std::string table_name_str = table_name_obj.get().convert();
      size_t table_size_num = table_size_obj.get().convert();

      table_name = table_name_str;
      table_size = table_size_num;

      iter_offset += (table_size + header_size + HEADER_SIZE_FIELD);
    }

  public:
    iterator(const std::string& file_path, bool seek_end = false) :
      fs(), file_size(0)
    {
      fs.open(file_path, std::ifstream::binary);

      fs.seekg(0, fs.end);
      file_size = fs.tellg();
      iter_offset += file_size;

      LOG_INFO_FMT("Snapshot file size: {}", file_size);

      if (!seek_end)
      {
        fs.seekg(0, fs.beg);
        iter_offset -= file_size;

        read_table_header();
      }
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

      fs.seekg(iter_offset);
      read_table_header();

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

    template <typename K, typename V>
    std::shared_ptr<TableSnapshot<K, V>> get_table_snapshot()
    {
      char* buffer = new char[table_size];

      if (!fs.read(buffer, table_size))
      {
        LOG_INFO_FMT("Error: Could not read table");
        throw std::logic_error("Snapshot read failed");
      }

      return std::make_shared<TableSnapshot<K, V>>(
        buffer, table_size, table_name);
    }

    std::string& operator*()
    {
      return table_name;
    }
  };

  iterator begin()
  {
    return iterator(file_path);
  }

  iterator end()
  {
    return iterator(file_path, true);
  }
};