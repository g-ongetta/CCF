// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.

#include <string>
#include <memory>
#include <fstream>

class Transaction
{
public:
    bool operator==(Transaction other) const {
        
    }
};

class Ledger
{
private:
    const std::string ledger_path;

public:
    Ledger(std::string ledger_path);

    class iterator : public std::iterator<std::input_iterator_tag, 
                                          Transaction, 
                                          Transaction, 
                                          const Transaction*,
                                          Transaction&>
    {

    private:
        std::ifstream fs;
        double file_size;
        double offset;
        std::shared_ptr<Transaction> current;

    public:
        iterator(std::string ledger_path, bool seek_end=false)
        : fs()
        , file_size(0)
        , offset(0)
        , current(nullptr)
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
                return;
            }

            fs.seekg(offset); // TODO: check if relative pos parameter is needed
            current.reset();
            return *this;
        }

        iterator operator++(int)
        {
            iterator retval = *this; ++(*this);
            return retval;
        }

        bool operator==(iterator other) const
        {
            return offset == other.offset;
        }

        bool operator!=(iterator other) const
        {
            return !(*this == other);
        }

        // reference operator*() const {
        //     return current;
        // }
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