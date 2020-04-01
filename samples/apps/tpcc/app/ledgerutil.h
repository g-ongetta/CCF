// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#include <string>
#include <memory>
#include <fstream>
#include <msgpack/msgpack.hpp>
#include "ds/serializer.h"

namespace
{
    static const size_t TRANSACTION_SIZE = 4;
    static const size_t DOMAIN_SIZE = 8;
    static const size_t GCM_SIZE_TAG = 16;
    static const size_t GCM_SIZE_IV = 12;
    static const size_t GCM_TOTAL_SIZE = GCM_SIZE_TAG + GCM_SIZE_IV;
}

class LedgerDomain
{

private:
    char * buffer;
    size_t length;
    size_t offset;

    uint64_t version;

    struct kv_update {
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
    LedgerDomain(char * buffer, size_t length)
    : buffer(buffer)
    , length(length)
    , offset(0)
    , version() 
    , table_names()
    , table_updates()
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

            // std::shared_ptr<kv_updates> updates = std::make_shared<kv_updates>();

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
            }

            table_updates.insert(std::make_pair(map_name_str, std::move(updates)));
        }
    }

    std::vector<std::string> get_table_names()
    {
        return table_names;
    }

    template <typename K, typename V>
    std::map<K, V> get_table_updates(std::string table_name)
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

    class iterator : public std::iterator<std::input_iterator_tag, 
                                          LedgerDomain, 
                                          LedgerDomain, 
                                          const LedgerDomain*,
                                          LedgerDomain&>
    {

    private:
        std::ifstream fs;
        size_t file_size;
        size_t offset;
        size_t domain_size;
        std::shared_ptr<LedgerDomain> current_domain;

        template <typename T>
        std::tuple<T> deserialize(char * buffer, const size_t size)
        {
            const uint8_t * data_ptr = (uint8_t*) buffer;
            size_t size_cpy = size;

            return serializer::CommonSerializer::deserialize<T>(data_ptr, size);
        }

        void read_header()
        {
            // Read transaction
            char * txn_buffer = new char[TRANSACTION_SIZE];
            if (!fs.read(txn_buffer, TRANSACTION_SIZE))
                LOG_INFO_FMT("Ledger Read Error: Could not read transaction");

            // Deserialize transaction
            std::tuple<uint32_t> txn = deserialize<uint32_t>(txn_buffer, TRANSACTION_SIZE);
            uint32_t txn_size = std::get<0>(txn);
            delete[] txn_buffer;

            // Update iterator offset
            offset += (txn_size + TRANSACTION_SIZE);

            // Read AES GCM header
            char * gcm_buffer = new char[GCM_TOTAL_SIZE];
            fs.read(gcm_buffer, GCM_TOTAL_SIZE);
            // TODO: unpack buffer for GCM header
            delete[] gcm_buffer;

            // Read public domain
            char * domain_buffer = new char[DOMAIN_SIZE];
            if (!fs.read(domain_buffer, DOMAIN_SIZE))
                LOG_INFO_FMT("Ledger Read Error: Could not read public domain header");
            
            // Deserialise public domain
            std::tuple<uint64_t> domain = deserialize<uint64_t>(domain_buffer, DOMAIN_SIZE);
            domain_size = std::get<0>(domain); 
            delete[] domain_buffer;
        }

    public:
        iterator(std::string ledger_path, bool seek_end=false)
        : fs()
        , file_size(0)
        , offset(0)
        , current_domain(nullptr)
        {
            fs.open(ledger_path, std::ifstream::binary);

            // Find the file length
            fs.seekg(0, fs.end);
            file_size = fs.tellg();
            offset += file_size;

            if (!seek_end) {
                fs.seekg(0, fs.beg);
                offset -= file_size;
                
                LOG_INFO_FMT("Ledger file size: {}", file_size);
                read_header();
            }
        }

        void seek_end()
        {
            fs.seekg(0, fs.end);
            offset = file_size;
        }
        
        iterator& operator++()
        {
            if (offset == file_size)
            {
                return *this;
            }

            fs.seekg(offset);
            current_domain.reset();

            read_header();
            return *this;
        }

        bool operator==(iterator other) const
        {
            return offset == other.offset;
        }

        bool operator!=(iterator other) const
        {
            return offset != other.offset;
        }

        LedgerDomain& operator*() {
            if (current_domain == nullptr)
            {
                char * buffer = new char[domain_size];
                if (!fs.read(buffer, domain_size))
                    LOG_INFO_FMT("Ledger Read Error: Could not read public domain");
                
                current_domain = std::make_shared<LedgerDomain>(buffer, domain_size);
            }

            return *current_domain;
        }
    };

    iterator begin()
    {
        return iterator(ledger_path);
    }

    iterator end()
    {
        return iterator(ledger_path, true);
    }

};