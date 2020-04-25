// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#include <msgpack/msgpack.hpp>
#include "tpcc_entities.h"
#include "ds/serializer.h"

using namespace ccf;

class Snapshot
{
private:

    std::ofstream fs;
    Store::Tx& tx;

    uint32_t buf_size(std::stringstream& buf)
    {
        buf.seekg(0, buf.end);
        int size = buf.tellg();
        buf.seekg(0, buf.beg);

        return size;
    }

public:

    Snapshot(std::string path, Store::Tx& tx)
    : fs(path, std::ofstream::out)
    , tx(tx)
    {}

    template <typename K, typename V>
    void serialize_table(Store::Map<K, V>& table, std::string name)
    {
        if (!fs.is_open())
        {
            throw std::logic_error("Serialize Error: Snapshot has been completed");
        }

        auto view = tx.get_view(table);

        std::stringstream data_buf;

        view->foreach([&](const auto& key, const auto& val) {
            msgpack::zone key_zone;
            msgpack::zone val_zone;

            msgpack::object key_obj(key, key_zone);
            msgpack::object val_obj(val, val_zone);

            msgpack::pack(data_buf, key_obj);
            msgpack::pack(data_buf, val_obj);

            return true;
        });

        std::stringstream header_buf;
        msgpack::pack(header_buf, name);
        msgpack::pack(header_buf, buf_size(data_buf));

        uint32_t header_size = buf_size(header_buf);

        fs << header_size
           << header_buf.rdbuf()
           << data_buf.rdbuf();
    }

    void complete_snapshot()
    {
        fs.close();
    }
};


template <typename K, typename V>
class TableSnapshot
{
private:

    char * buffer;
    size_t length;
    size_t offset;

    std::string table_name;

    msgpack::unpacked unpack()
    {
        msgpack::unpacked result;
        msgpack::unpack(result, buffer, length, offset);
        return result;
    }

public:
    TableSnapshot(char * buffer, size_t length, std::string name)
    : buffer(buffer)
    , length(length)
    , offset(0)
    , table_name(name)
    {
        while (offset != length)
        {
            msgpack::unpacked key = unpack();
            msgpack::unpacked val = unpack();

            K w = key.get().convert();
            V wh = val.get().convert();

            LOG_INFO_FMT("Warehouse ID: {}, name: {}, street_1: {}, zip: {}", w, wh.name, wh.street_1, wh.zip);
        }
    }

    std::string get_name()
    {
        return table_name;
    }
};

class SnapshotReader
{
private:
    std::string file_path;

public:
    SnapshotReader(const std::string& file_path) : file_path(file_path) {}

    class iterator : public std::iterator<std::input_iterator_tag,
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
        uint32_t table_size;

        void read_table_header()
        {
            static const int HEADER_SIZE_FIELD = 4;

            // Read header size from file
            uint32_t header_size;
            fs >> header_size;

            LOG_INFO_FMT("Header Size: {}", header_size);

            // Read header from file
            char * header_buf = new char[header_size];
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

            LOG_INFO_FMT("Header - Name: {}, Size: {}", table_name, table_size);

            iter_offset += (table_size + header_size + HEADER_SIZE_FIELD);
        }
        
    public:
        iterator(const std::string& file_path, bool seek_end=false)
        : fs()
        , file_size(0)
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
            char * buffer = new char[table_size];

            if (!fs.read(buffer, table_size))
            {
                LOG_INFO_FMT("Error: Could not read table");
                throw std::logic_error("Snapshot read failed");
            }

            return std::make_shared<TableSnapshot<K,V>>(buffer, table_size, table_name);
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