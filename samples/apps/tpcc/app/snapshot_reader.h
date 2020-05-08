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
      fs(), file_size(0), iter_offset(0), table_name(), table_size(0)
    {
      fs.open(file_path, std::ifstream::binary);

      fs.seekg(0, fs.end);
      file_size = fs.tellg();
      iter_offset += file_size;

      if (!seek_end)
      {
        LOG_INFO_FMT("Reading {} - Size: {}", file_path, file_size);
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
      if (iter_offset > file_size)
      {
        LOG_INFO_FMT("Error: Snapshot iter offset exceeds file size");
        throw std::logic_error("Snapshot iterator is malformed");
      }
      
      if (iter_offset < file_size)
      {
        table_name = "";
        table_size = 0;

        fs.seekg(iter_offset);
        read_table_header();
      }

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

using Snapshots = Store::Map<uint64_t, std::vector<uint8_t>>;
