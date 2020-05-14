// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#pragma once

#include "consensus/pbft/libbyz/digest.h"
#include "kv.h"

#include <msgpack/msgpack.hpp>

namespace
{
  enum Action
  {
    WRITE = 0,
    REMOVE = 1
  };

  using KeyValueUpdate =
    std::tuple<std::vector<uint8_t>, std::vector<uint8_t>, Action>;
}

using SnapshotHashes = ccf::Store::Map<uint64_t, std::vector<uint8_t>>;

namespace kv
{
  class Snapshot
  {
  private:
    uint64_t version;
    uint64_t ledger_offset;
    std::string file_path;
    std::vector<uint8_t> hash;

    crypto::Sha256Hash merkle_root;

    // index ?

  public:
    Snapshot(
      uint64_t version,
      uint64_t ledger_offset,
      std::string file_path,
      std::vector<uint8_t> hash,
      crypto::Sha256Hash merkle_root)
    : version(version)
    , ledger_offset(ledger_offset)
    , file_path(file_path)
    , hash(hash)
    , merkle_root(merkle_root)
    {}

    uint64_t get_version() const
    {
      return version;
    }

    uint64_t get_ledger_offset() const
    {
      return ledger_offset;
    }

    std::vector<uint8_t> get_hash() const
    {
      return hash;
    }

    crypto::Sha256Hash get_merkle_root() const
    {
      return merkle_root;
    }
  };

  class SnapshotManager
  {
  private:
    std::vector<Snapshot> snapshots;

    // TODO: implement skip-list ?

  public:
    SnapshotManager() : snapshots() {}

    void append(Snapshot snapshot)
    {
      snapshots.push_back(snapshot);
    }

    std::vector<Snapshot> get_snapshots()
    {
      return snapshots;
    }
  };

  class SnapshotSerializer
  {
  private:
    std::ofstream fs;
    std::string file_path;

    Digest::Context context;
    Digest digest;

  public:
    SnapshotSerializer(std::string file_path) :
      fs(file_path, std::ofstream::binary), context(), digest()
    {}

    void serialize_table(
      const std::string& name, const std::deque<KeyValueUpdate>& updates)
    {
      std::unordered_set<std::vector<uint8_t>> added_keys;

      msgpack::sbuffer data_buffer;

      for (auto iter = updates.rbegin(); iter != updates.rend(); ++iter)
      {
        auto [key, val, action] = *iter;

        // TODO: handle removes
        if (action == Action::REMOVE)
          continue;

        if (added_keys.find(key) != added_keys.end())
          continue;

        added_keys.emplace(key);

        // Write is used rather than pack because this data is already packed
        data_buffer.write((char*)key.data(), key.size());
        data_buffer.write((char*)val.data(), val.size());
      }

      msgpack::sbuffer header_buffer;
      msgpack::pack(header_buffer, name);
      msgpack::pack(header_buffer, data_buffer.size());

      size_t header_size = header_buffer.size();
      size_t data_size = data_buffer.size();

      digest.update_last(context, header_buffer.data(), header_size);
      digest.update_last(context, data_buffer.data(), data_size);

      fs << header_size;

      if (!fs.write(header_buffer.data(), header_size))
      {
        LOG_INFO_FMT("Snapshot Error: Could not write header");
        throw std::logic_error("Snapshot creation error");
      }

      if (!fs.write(data_buffer.data(), data_size))
      {
        LOG_INFO_FMT("Snapshot Error: Could not write data");
        throw std::logic_error("Snapshot creation error");
      }
    }

    std::vector<uint8_t> finalize()
    {
      fs.close();
      digest.finalize(context);

      std::vector<uint8_t> hash_bytes;

      char* hash = digest.digest();
      for (int i = 0; i < 32; i++)
      {
        hash_bytes.push_back(hash[i]);
      }

      return hash_bytes;
    }
  };

  class SnapshotWriter
  {
  private:
    std::unordered_map<std::string, std::deque<KeyValueUpdate>> updates;
    uint64_t ledger_offset;

    msgpack::unpacked unpack(const uint8_t* data, size_t length, size_t& offset)
    {
      msgpack::unpacked obj;
      msgpack::unpack(obj, (char*)data, length, offset);
      return obj;
    }

    std::vector<uint8_t> unpack_bytes(
      const uint8_t* data, size_t length, size_t& offset)
    {
      size_t initial_offset = offset;
      msgpack::unpacked key = unpack(data, length, offset);

      size_t key_size = offset - initial_offset;
      std::vector<uint8_t> bytes;

      for (int i = 0; i < key_size; i++)
      {
        bytes.push_back(data[initial_offset + i]);
      }

      return bytes;
    }

    void append_update(std::string name, KeyValueUpdate update)
    {
      if (updates.find(name) == updates.end())
        updates.emplace(name, std::deque<KeyValueUpdate>{update});
      else
        updates[name].push_back(update);
    }

  public:
    SnapshotWriter() : updates(), ledger_offset(0) {}

    void append_transaction(const uint8_t* data, size_t length)
    {
      size_t offset = 0;

      offset += 28; // Seek past GCM Header
      offset += 8; // Seek past 'Public Domain Size' field

      unpack(data, length, offset); // Version (ignore)

      while (offset != length)
      {
        unpack(data, length, offset); // Map Start Indicator (ignore)

        msgpack::unpacked map_name = unpack(data, length, offset);
        std::string map_name_str = map_name.get().convert();

        unpack(data, length, offset); // Read Version (ignore)
        unpack(data, length, offset); // Read Count (ignore)

        msgpack::unpacked write_count = unpack(data, length, offset);
        size_t write_count_int = write_count.get().convert();

        for (auto i = 0; i < write_count_int; i++)
        {
          std::vector<uint8_t> key_bytes = unpack_bytes(data, length, offset);
          std::vector<uint8_t> val_bytes = unpack_bytes(data, length, offset);

          KeyValueUpdate update = std::make_tuple(key_bytes, val_bytes, WRITE);
          append_update(map_name_str, update);
        }

        msgpack::unpacked remove_count = unpack(data, length, offset);
        size_t remove_count_int = remove_count.get().convert();

        for (auto i = 0; i < remove_count_int; i++)
        {
          std::vector<uint8_t> key_bytes = unpack_bytes(data, length, offset);
          std::vector<uint8_t> val_bytes = {};

          KeyValueUpdate update = std::make_tuple(key_bytes, val_bytes, REMOVE);
          append_update(map_name_str, update);
        }
      }

      static const size_t SIZE_FIELD = 4;
      ledger_offset += (offset + SIZE_FIELD);
    }

    Snapshot create(uint64_t version, crypto::Sha256Hash merkle_root)
    {
      std::string snapshot_file = fmt::format("snapshot_v{}", version);
      SnapshotSerializer serializer(snapshot_file);

      for (auto iter = updates.begin(); iter != updates.end(); ++iter)
      {
        std::string name = iter->first;
        std::deque<KeyValueUpdate> update_queue = iter->second;

        serializer.serialize_table(name, update_queue);
      }

      std::vector<uint8_t> hash = serializer.finalize();

      return Snapshot(version, ledger_offset, snapshot_file, hash, merkle_root);
    }
  };

} // namespace kv