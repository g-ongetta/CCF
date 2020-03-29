// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#include <string>
#include <memory>
#include <fstream>
#include <msgpack/msgpack.hpp>
#include "ds/serialized.h"

namespace
{
    static size_t TRANSACTION_SIZE = 4;
    static size_t DOMAIN_SIZE = 8;
    static size_t GCM_SIZE_TAG = 16;
    static size_t GCM_SIZE_IV = 12;
    static size_t GCM_TOTAL_SIZE = GCM_SIZE_TAG + GCM_SIZE_IV;
}

class LedgerDomain
{

private:
    // TODO what type should tables be
    
    char * buffer;
    size_t length;
    size_t offset;

    msgpack::object version;

    // std::map<std::string, std::map<msgpack::object, msgpack::object>> tables;

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
    , length(0)
    , offset(0)
    , version(unpack().get())
    , table_names()
    // , tables()
    {
        // Read tables
        while (offset != length)
        {
            msgpack::unpacked map_start_indicator = unpack();
            msgpack::unpacked map_name = unpack();
            std::string map_name_str = map_name.get().convert();
            table_names.push_back(map_name_str);

            msgpack::unpacked read_version = unpack();
            msgpack::unpacked read_count = unpack();

            // std::map<msgpack::object, msgpack::object> table;

            msgpack::unpacked write_count = unpack();
            size_t write_count_num = write_count.get().convert();
            for (auto i = 0; i < write_count_num; i++)
            {
                msgpack::unpacked key = unpack();
                msgpack::unpacked val = unpack();
                // table.emplace(key.get(), val.get());
            }

            msgpack::unpacked remove_count = unpack();
            size_t remove_count_num = remove_count.get().convert();
            for (auto i = 0; i < remove_count_num; i++)
            {
                msgpack::unpacked key = unpack();
                // table.erase(key.get());
            }

            // tables.emplace(map_name.get().convert(), table);
        }
    }

    // std::map<std::string, std::map<msgpack::object, msgpack::object>> get_tables() const
    // {
    //     return tables;
    // }
};

class Ledger
{
private:
    const std::string ledger_path;

public:
    Ledger(std::string ledger_path);

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

        void read_header()
        {
            // Read transaction
            char * txn_buffer = new char[TRANSACTION_SIZE];
            fs.read(txn_buffer, TRANSACTION_SIZE);

            const uint8_t* txn_data = (uint8_t*) txn_buffer;
            auto txn_size = serialized::read<uint32_t>(txn_data, TRANSACTION_SIZE);

            offset += (txn_size + TRANSACTION_SIZE);
            delete[] txn_buffer;


            // Read AES GCM header
            char * gcm_buffer = new char[GCM_TOTAL_SIZE];
            fs.read(gcm_buffer, GCM_TOTAL_SIZE);
            // TODO: unpack buffer for GCM header
            delete[] gcm_buffer;


            // Read public domain
            char * domain_buffer = new char[DOMAIN_SIZE];
            fs.read(domain_buffer, DOMAIN_SIZE);
            
            const uint8_t* domain_data = (uint8_t*)domain_buffer;
            domain_size = serialized::read<uint64_t>(domain_data, TRANSACTION_SIZE);
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

            if (!seek_end) {
                fs.seekg(0, fs.beg);
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

            fs.seekg(offset); // TODO: check if relative pos parameter is needed
            current_domain.reset();

            read_header();
            return *this;
        }

        // iterator operator++(int)
        // {
        //     iterator retval = *this;
        //     ++(*this);
        //     return retval;
        // }

        // bool operator==(iterator other) const
        // {
        //     return offset == other.offset;
        // }

        // bool operator!=(iterator other) const
        // {
        //     return !(*this == other);
        // }

        LedgerDomain& operator*() {
            if (current_domain == nullptr)
            {
                char * buffer = new char[domain_size];
                fs.read(buffer, domain_size);
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