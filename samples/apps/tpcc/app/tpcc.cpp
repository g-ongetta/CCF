// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#include "enclave/app_interface.h"
#include "node/rpc/user_frontend.h"
#include "node/history.h"
#include "node/signatures.h"

#include "tpcc_entities.h"
#include "ledgerutil.h"
#include "snapshot.h"

#include <chrono>

using namespace ccf;

namespace ccfapp
{
namespace tpcc
{

  struct Procs {
    static constexpr auto TPCC_NEW_ORDER = "TPCC_new_order";

    static constexpr auto TPCC_QUERY_HISTORY = "TPCC_query_history";
    static constexpr auto TPCC_LEDGER_VERIFY = "TPCC_ledger_verify";
    static constexpr auto TPCC_KV_SNAPSHOT = "TPCC_kv_snapshot";
    
    static constexpr auto TPCC_LOAD_ITEMS = "TPCC_load_items";
    static constexpr auto TPCC_LOAD_WAREHOUSE = "TPCC_load_warehouse";
    static constexpr auto TPCC_LOAD_STOCKS = "TPCC_load_stocks";
    static constexpr auto TPCC_LOAD_DISTRICT = "TPCC_load_district";
    static constexpr auto TPCC_LOAD_CUSTOMER = "TPCC_load_customer";
    static constexpr auto TPCC_LOAD_HISTORY = "TPCC_load_history";
    static constexpr auto TPCC_LOAD_ORDER = "TPCC_load_order";
    static constexpr auto TPCC_LOAD_ORDER_LINES = "TPCC_load_order_lines";
    static constexpr auto TPCC_LOAD_NEW_ORDERS = "TPCC_load_new_orders";
  };

  struct TpccTables
  {
    Store::Map<WarehouseId, Warehouse>& warehouses;
    Store::Map<DistrictId, District>& districts;
    Store::Map<CustomerId, Customer>& customers;
    Store::Map<HistoryId, History>& histories;
    Store::Map<NewOrderId, NewOrder>& neworders;
    Store::Map<OrderId, Order>& orders;
    Store::Map<OrderLineId, OrderLine>& orderlines;
    Store::Map<ItemId, Item>& items;
    Store::Map<StockId, Stock>& stocks;

    ccf::Nodes* nodes;
    ccf::Signatures* sigs;
  
    TpccTables(Store& store) :
      warehouses(store.create<WarehouseId, Warehouse>("warehouses", kv::PUBLIC)),
      districts(store.create<DistrictId, District>("districts", kv::PUBLIC)),
      customers(store.create<CustomerId, Customer>("customers", kv::PUBLIC)),
      histories(store.create<HistoryId, History>("histories", kv::PUBLIC)),
      neworders(store.create<NewOrderId, NewOrder>("neworders", kv::PUBLIC)),
      orders(store.create<OrderId, Order>("orders", kv::PUBLIC)),
      orderlines(store.create<OrderLineId, OrderLine>("orderlines", kv::PUBLIC)),
      items(store.create<ItemId, Item>("items", kv::PUBLIC)),
      stocks(store.create<StockId, Stock>("stocks", kv::PUBLIC)),
      nodes(store.get<Nodes>(ccf::Tables::NODES)),
      sigs(store.get<Signatures>(ccf::Tables::SIGNATURES))
    {}
  };

  using TimePoint = std::chrono::system_clock::time_point;

  class TpccHandlers : public UserHandlerRegistry
  {
  private:
    TpccTables tables;

    // Performs query over history table in Key Value Store
    void query_history_kv(TimePoint date_from, TimePoint date_to,
        Store::Tx& tx, std::vector<uint64_t>& results)
    {
      LOG_INFO << "Processing History Query via KV Store" << std::endl;

      auto history_view = tx.get_view(tables.histories);
      history_view->foreach([&](const auto& key, const auto& val) {

        // Parse date of History entry
        std::tm date_tm = {};
        std::istringstream ss_to(val.date);
        ss_to >> std::get_time(&date_tm, "%F %T");

        // Convert date to time point for comparison
        TimePoint date = std::chrono::system_clock::from_time_t(mktime(&date_tm));

        // Check if date of History entry lies within range, if so add to results
        if (date <= date_to && date >= date_from) {
          results.push_back(val.c_id);
        }

        return true;
      });
    }

    // Performs query over history using ledger replay technique
    void query_history_ledger(TimePoint date_from, TimePoint date_to, std::vector<uint64_t>& results)
    {
      LOG_INFO << "Processing History Query via Ledger Replay" << std::endl;

      std::string path = "0.ledger";
      Ledger ledger_reader(path);

      // Tracks when last update is found
      bool exceeded_range = false;

      // Start querying from beginning of ledger, until last update is found that satisfies
      for (auto iter = ledger_reader.begin(); iter <= ledger_reader.end(); ++iter)
      {
        LedgerDomain& domain = *iter;

        std::vector<std::string> tables = domain.get_table_names();

        // Continue if no history updates in current transaction
        if (std::find(tables.begin(), tables.end(), "histories") == tables.end())
          continue;

        auto updates = domain.get_table_updates<HistoryId, History>("histories");

        for (auto updates_iter = updates.begin(); updates_iter != updates.end(); ++updates_iter)
        {
          HistoryId key = updates_iter->first;
          History val = updates_iter->second;

          // Parse date of History entry
          std::tm date_tm = {};
          std::istringstream ss_to(val.date);
          ss_to >> std::get_time(&date_tm, "%F %T");

          // Convert date to time_point for comparison
          TimePoint date = std::chrono::system_clock::from_time_t(mktime(&date_tm));

          // Check if date of History entry lies within 'from' range
          if (date >= date_from)
          {
            // If date also within 'to' range, add to results, else stop search
            if (date <= date_to)
            {
              results.push_back(val.c_id);
            }
            else
            {
              exceeded_range = true;
              break;
            }
          }
        }

        if (exceeded_range)
          break;
      }
    }

  public:
    TpccHandlers(Store& store) : UserHandlerRegistry(store), tables(store)
    {}

    void init_handlers(Store& store) override
    {
      UserHandlerRegistry::init_handlers(store);

      auto kvSnapshot = [this](Store::Tx& tx, const nlohmann::json& params) {

        LOG_INFO << "Processing KV Snapshot..." << std::endl;

        Snapshot snapshot("snapshot.txt", tx);
        snapshot.serialize_table<WarehouseId, Warehouse>(tables.warehouses, "warehouses");
        snapshot.complete_snapshot();

        SnapshotReader reader("snapshot.txt");
        auto iter = reader.begin();

        if (*iter == "warehouses")
        {
          auto snapshot = *iter.get_table_snapshot<WarehouseId, Warehouse>();
        }

        return make_success(true);
      };

      auto ledgerVerify = [this](Store::Tx& tx, const nlohmann::json& params) {

        std::string path = "0.ledger";
        Ledger ledger(path);

        MerkleTreeHistory merkle_history;

        for (auto iter = ledger.begin(); iter <= ledger.end(); ++iter)
        {
          // Get table updates for current transaction
          LedgerDomain& domain = *iter;
          std::vector<std::string> t_names = domain.get_table_names();

          // Search for update to signatures table (implying new batch)
          if (std::find(t_names.begin(), t_names.end(), "ccf.signatures") != t_names.end())
          {
            uint64_t version = domain.get_version();

            // Flush/truncate the Merkle tree if version exceeds max length
            if (version >= MAX_HISTORY_LEN)
            {
              merkle_history.flush(domain.get_version() - MAX_HISTORY_LEN);
            }

            // Verify the root of our Merkle tree with the new signature
            auto updates = domain.get_table_updates<ObjectId, Signature>("ccf.signatures");
            auto updates_iter = updates.begin();

            // Sig is first entry in iter since only one signature will exist
            Signature sig = updates_iter->second;

            // Find the node that created the signature
            auto node = tx.get_view(*tables.nodes)->get(sig.node);
            if (!node.has_value())
            {
              LOG_INFO_FMT("ERROR: Node has no value");
              return make_error(HTTP_STATUS_INTERNAL_SERVER_ERROR, "Could not read node");
            }

            // Verify using node's certificate
            tls::VerifierPtr verifier = tls::make_verifier(node.value().cert);
            crypto::Sha256Hash merkle_root = merkle_history.get_root();

            bool verified = verifier->verify_hash(
              merkle_root.h.data(),
              merkle_root.h.size(),
              sig.sig.data(),
              sig.sig.size()
            );

            if (!verified)
            {
              LOG_INFO_FMT("Error: Verification for TXN {} FAILED", version);
              return make_error(HTTP_STATUS_INTERNAL_SERVER_ERROR, "Could not verify ledger contents");
            }
          }

          // Append ledger data to merkle tree
          auto [data, size] = iter.get_raw_data();
          crypto::Sha256Hash hash({{data, size}});
          merkle_history.append(hash);
        }

        // TODO: verify final state by just looking at merkle root (it wouldnb't be signed yet)

        return make_success(true);
      };

      auto queryOrderHistory = [this](Store::Tx& tx, const nlohmann::json& params) {
        LOG_INFO << "Processing history query..." << std::endl;

        std::string method_str = params["method"];
        std::string date_from_str = params["date_from"];
        std::string date_to_str = params["date_to"];

        LOG_INFO_FMT("Input date params: {} to {}", date_from_str, date_to_str);
        
        // Validate method input parameter
        if (!(method_str == "kv" || method_str == "ledger"))
        {
          LOG_INFO_FMT("Error: Invalid Query Method {}", method_str);
          return make_error(HTTP_STATUS_BAD_REQUEST, "Invalid query method");
        }

        // Parse date_from input parameter
        std::tm date_from_tm = {};
        std::istringstream ss_from(date_from_str);
        ss_from >> std::get_time(&date_from_tm, "%F %T");

        // Parse date_to input parameter
        std::tm date_to_tm = {};
        std::istringstream ss_to(date_to_str);
        ss_to >> std::get_time(&date_to_tm, "%F %T");

        // Check that both date parameters were correctly parsed
        if (ss_from.fail() || ss_to.fail()) {
          LOG_INFO_FMT("Could not parse date input: From: {} To: {}", date_from_str, date_to_str);
          return make_error(HTTP_STATUS_BAD_REQUEST, "Could not parse date parameter");
        }
        
        // Convert input dates to time points for comparison
        TimePoint date_from = std::chrono::system_clock::from_time_t(mktime(&date_from_tm));
        TimePoint date_to = std::chrono::system_clock::from_time_t(mktime(&date_to_tm));

        if (date_to < date_from) {
          LOG_INFO_FMT("Error! From date: {} must be before To date: {}", date_from_str, date_to_str);
          return make_error(HTTP_STATUS_BAD_REQUEST, "From date must be before To date");
        }

        std::vector<uint64_t> results{};

        if (method_str == "kv")
        {
          query_history_kv(date_from, date_to, tx, results);
        }
        else if (method_str == "ledger")
        {
          query_history_ledger(date_from, date_to, results);
        }
        else
        {
          LOG_INFO_FMT("Invalid Query Method: {}", method_str);
          return make_error(HTTP_STATUS_BAD_REQUEST, "Invalid Query Method");
        }
 
        LOG_INFO_FMT("Query found {} entries", results.size());

        return make_success(true);
      };

      auto newOrder = [this](Store::Tx& tx, const nlohmann::json& params) {
        // LOG_INFO << "Executing newOrder handler...";

        uint64_t w_id = params["w_id"];
        uint64_t d_id = params["d_id"];
        uint64_t c_id = params["c_id"];
        std::string o_entry_d = params["o_entry_d"];
        std::vector<uint64_t> i_ids = params["i_ids"];
        std::vector<uint64_t> i_w_ids = params["i_w_ids"];
        std::vector<uint64_t> i_qtys = params["i_qtys"];

        // Output data defined as per TPCC 2.4.3.3
        nlohmann::json output_data;
        output_data["w_id"] = w_id;
        output_data["d_id"] = d_id;
        output_data["c_id"] = c_id;
        output_data["o_entry_d"] = o_entry_d;

        // Get district information
        auto districts_view = tx.get_view(tables.districts);

        DistrictId district_key = {d_id, w_id};
        auto d_result = districts_view->get(district_key);

        if (!d_result.has_value())
        {
          LOG_INFO << "Error! District not found" << std::endl;
          return make_error(HTTP_STATUS_BAD_REQUEST, "District Not Found");
        }

        District& d = d_result.value();
        double d_tax = d.tax;
        uint64_t d_next_o_id = d.next_o_id;

        output_data["d_tax"] = d_tax;
        output_data["o_id"] = d_next_o_id;

        // Update the district's next order number
        d.next_o_id += 1;
        districts_view->put(district_key, d);
      
        // Get warehouse information
        auto warehouses_view = tx.get_view(tables.warehouses);

        WarehouseId warehouse_key = w_id;
        auto w_result = warehouses_view->get(warehouse_key);
        
        if (!w_result.has_value())
        {
          LOG_INFO << "Error! Warehouse not found" << std::endl;
          return make_error(HTTP_STATUS_BAD_REQUEST, "Warehouse Not Found");
        }

        Warehouse& w = w_result.value();
        double w_tax = w.tax;

        output_data["w_tax"] = w_tax;

        // Get customer information
        auto customers_view = tx.get_view(tables.customers);

        CustomerId customer_key = {c_id, w_id, d_id};
        auto c_result = customers_view->get(customer_key);

        if (!c_result.has_value())
        {
          LOG_INFO << "Error! Customer not found" << std::endl;
          return make_error(HTTP_STATUS_BAD_REQUEST, "Customer Not Found");
        }

        Customer& c = c_result.value();
        double c_discount = c.discount;
        std::string& c_last = c.last;
        std::string& c_credit = c.credit;

        output_data["c_last"] = c_last;
        output_data["c_credit"] = c_credit;
        output_data["c_discount"] = c_discount;

        // Insert NewOrder entry
        auto neworders_view = tx.get_view(tables.neworders);

        NewOrderId neworder_key = {d_next_o_id, w_id, d_id};
        NewOrder no = {0};
        neworders_view->put(neworder_key, no);

        // Insert Order entry
        auto orders_view = tx.get_view(tables.orders);

        uint8_t all_local = 0; //TODO: set this appropriately
        uint64_t ol_cnt = i_ids.size();

        output_data["o_ol_cnt"] = ol_cnt;

        OrderId order_key = {d_next_o_id, w_id, d_id};
        Order order = {
          c_id,
          o_entry_d,
          0, // carrier_id unused for benchmark purposes
          ol_cnt,
          all_local
        };

        // Insert Order Line and Stock Information
        auto items_view = tx.get_view(tables.items);
        auto stocks_view = tx.get_view(tables.stocks);
        auto orderlines_view = tx.get_view(tables.orderlines);

        uint64_t total = 0;

        std::vector<nlohmann::json> item_output_data;
        item_output_data.reserve(ol_cnt);

        for (size_t i = 0; i < ol_cnt; i++)
        {
          uint64_t i_id = i_ids.at(i);
          uint64_t i_w_id = i_w_ids.at(i);
          uint64_t ol_quantity = i_qtys.at(i);

          // Stores required output data for item as per TPCC 2.4.3.3
          nlohmann::json item_data;
          item_data["ol_supply_w_id"] = i_w_id;
          item_data["ol_i_id"] = i_id;
          item_data["ol_quantity"] = ol_quantity;

          // Find the ITEM
          auto i_result = items_view->get(i_id);

          if (!i_result.has_value())
          {
            // 'not-found' signal, item was not found in store
            LOG_INFO << "Error! Item not found. Key: " << i_id << std::endl;
            return make_error(HTTP_STATUS_BAD_REQUEST, "Item Not Found");
          }

          Item& item = i_result.value();
          double i_price = item.price;
          std::string& i_name = item.name;
          std::string& i_data = item.data;

          item_data["i_name"] = i_name;
          item_data["i_price"] = i_price;

          // Find the STOCK
          StockId stock_key = {i_w_id, i_id};
          auto s_result = stocks_view->get(stock_key);

          if (!s_result.has_value())
          {
            LOG_INFO << "Error! Stock not found. Key: (" << i_w_id << ", " << i_id << ")" << std::endl;
            return make_error(HTTP_STATUS_BAD_REQUEST, "Stock Not Found");
          }

          Stock& stock = s_result.value();

          // Update stock information
          if (stock.quantity >= ol_quantity + 10)
          {
            stock.quantity -= ol_quantity;
          }
          else
          {
            stock.quantity = stock.quantity - ol_quantity + 91;
          }

          stock.ytd += ol_quantity;
          stock.order_cnt += 1;

          if (i_w_id != w_id)
          {
            stock.remote_cnt += 1;
          }

          stocks_view->put(stock_key, stock);
          item_data["s_quantity"] = stock.quantity;

          // Check the data for the 'brand-generic' field
          char brand_generic;
          if (i_data.find("ORIGINAL") != std::string::npos 
              && stock.data.find("ORIGINAL") != std::string::npos)
          {
            brand_generic = 'B';
          }
          else
          {
            brand_generic = 'G';
          }

          item_data["brand_generic"] = brand_generic;

          // Insert the OrderLine entry
          uint64_t ol_amount = ol_quantity * i_price;

          item_data["ol_amount"] = ol_amount;
          total += ol_amount;

          OrderLineId orderline_key = {d_next_o_id, w_id, d_id, i};
          OrderLine orderline = {
            i_id,
            i_w_id,
            "",
            (uint8_t) stock.quantity,
            (double) ol_amount,
            stock.dist_xx[d_id - 1]
          };

          orderlines_view->put(orderline_key, orderline);
          item_output_data.push_back(item_data);
        }

        total *= (1 - c_discount) * (1 + w_tax + d_tax);

        orders_view->put(order_key, order);

        output_data["item_data"] = item_output_data;
        output_data["total_amount"] = total;
        output_data["status_msg"] = "Success";

        // LOG_INFO << "done" << std::endl;
        return make_success(output_data);
      };

      auto loadItems = [this](Store::Tx& tx, const nlohmann::json& params) {
        //LOG_INFO << "Executing loadItems handler...";

        auto items_view = tx.get_view(tables.items);
        int load_count = 0;

        for (auto& element : params) {
          ItemId key = element["key"];

          Item item;
          item.name = element["value"]["name"];
          item.data = element["value"]["data"];
          item.price = element["value"]["price"];
          item.im_id = element["value"]["im_id"];

          items_view->put(key, item);
          load_count++;
        }

        //LOG_INFO << "done" << std::endl;
        return make_success(true);
      };
      
      auto loadWarehouse = [this](Store::Tx& tx, const nlohmann::json& params) {
        //LOG_INFO << "Executing loadWarehouse handler...";
        auto warehouses_view = tx.get_view(tables.warehouses);
        
        WarehouseId key = params["key"];
        Warehouse warehouse;
        warehouse.name = params["value"]["name"];
        warehouse.street_1 = params["value"]["street_1"];
        warehouse.street_2 = params["value"]["street_2"];
        warehouse.city = params["value"]["city"];
        warehouse.state = params["value"]["state"];
        warehouse.zip = params["value"]["zip"];
        warehouse.tax = params["value"]["tax"];
        warehouse.ytd = params["value"]["ytd"];

        warehouses_view->put(key, warehouse);

        //LOG_INFO << "done" << std::endl;
        return make_success(true);
      };

      auto loadStocks = [this](Store::Tx& tx, const nlohmann::json& params) {
        //LOG_INFO << "Executing loadStocks handler...";
        auto stocks_view = tx.get_view(tables.stocks);
        int load_count = 0;

        for (auto& element : params) {
          StockId key;
          key.w_id = element["key"]["w_id"];
          key.i_id = element["key"]["i_id"];

          Stock stock;
          stock.quantity = element["value"]["quantity"];
          stock.ytd = element["value"]["ytd"];
          stock.order_cnt = element["value"]["order_cnt"];
          stock.remote_cnt = element["value"]["remote_cnt"];
          stock.data = element["value"]["data"];
          
          for (int i = 0; i < 10; i++)
          {
            stock.dist_xx[i] = element["value"]["dist_xx"][i];
          }

          stocks_view->put(key, stock);
          load_count++;
        }

        //LOG_INFO << "done" << std::endl;
        return make_success(load_count);
      };

      auto loadDistrict = [this](Store::Tx& tx, const nlohmann::json& params) {
        //LOG_INFO << "Executing loadDistrict handler...";
        auto districts_view = tx.get_view(tables.districts);

        DistrictId key;
        key.id = params["key"]["id"];
        key.w_id = params["key"]["w_id"];

        District district;
        district.name = params["value"]["name"];
        district.street_1 = params["value"]["street_1"];
        district.street_2 = params["value"]["street_2"];
        district.city = params["value"]["city"];
        district.state = params["value"]["state"];
        district.zip = params["value"]["zip"];
        district.tax = params["value"]["tax"];
        district.ytd = params["value"]["ytd"];
        district.next_o_id = params["value"]["next_o_id"];

        districts_view->put(key, district);

        //LOG_INFO << "done" << std::endl;
        return make_success(true);
      };

      auto loadCustomer = [this](Store::Tx& tx, const nlohmann::json& params) {
        //LOG_INFO << "Executing loadCustomer handler...";
        auto customers_view = tx.get_view(tables.customers);

        CustomerId key;
        key.id = params["key"]["id"];
        key.w_id = params["key"]["w_id"];
        key.d_id = params["key"]["d_id"];

        Customer customer;
        customer.first = params["value"]["first"];
        customer.middle = params["value"]["middle"];
        customer.last = params["value"]["last"];
        customer.street_1 = params["value"]["street_1"];
        customer.street_2 = params["value"]["street_2"];
        customer.city = params["value"]["city"];
        customer.state = params["value"]["state"];
        customer.zip = params["value"]["zip"];
        customer.phone = params["value"]["phone"];
        customer.since = params["value"]["since"];
        customer.credit = params["value"]["credit"];
        customer.credit_lim = params["value"]["credit_lim"];
        customer.discount = params["value"]["discount"];
        customer.balance = params["value"]["balance"];
        customer.ytd_payment = params["value"]["ytd_payment"];
        customer.payment_cnt = params["value"]["payment_cnt"];
        customer.delivery_cnt = params["value"]["delivery_cnt"];
        customer.data = params["value"]["data"];

        customers_view->put(key, customer);

        //LOG_INFO << "done" << std::endl;
        return make_success(true);
      };

      auto loadHistory = [this](Store::Tx& tx, const nlohmann::json& params) {
        //LOG_INFO << "Executing loadHistory handler...";
        auto histories_view = tx.get_view(tables.histories);

        HistoryId key = params["key"];

        History history;
        history.c_id = params["value"]["c_id"];
        history.c_d_id = params["value"]["c_d_id"];
        history.c_w_id = params["value"]["c_w_id"];
        history.d_id = params["value"]["d_id"];
        history.w_id = params["value"]["w_id"];
        history.date = params["value"]["date"];
        history.amount = params["value"]["amount"];
        history.data = params["value"]["data"];

        histories_view->put(key, history);

        //LOG_INFO << "done" << std::endl;
        return make_success(true);
      };

      auto loadOrder = [this](Store::Tx& tx, const nlohmann::json& params) {
        //LOG_INFO << "Executing loadOrder handler...";
        auto orders_view = tx.get_view(tables.orders);

        OrderId key;
        key.id = params["key"]["id"];
        key.w_id = params["key"]["w_id"];
        key.d_id = params["key"]["d_id"];
        
        Order order;
        order.c_id = params["value"]["c_id"];
        order.entry_d = params["value"]["entry_d"];
        order.carrier_id = params["value"]["carrier_id"];
        order.ol_cnt = params["value"]["ol_cnt"];
        order.all_local = params["value"]["all_local"];

        orders_view->put(key, order);

        //LOG_INFO << "done" << std::endl;
        return make_success(true);
      };

      auto loadOrderLines = [this](Store::Tx& tx, const nlohmann::json& params) {
        //LOG_INFO << "Executing loadOrderLines handler...";
        auto order_lines_view = tx.get_view(tables.orderlines);
        int load_count = 0;

        for (auto& element : params)
        {            
          OrderLineId key;
          key.o_id = element["key"]["o_id"];
          key.w_id = element["key"]["w_id"];
          key.d_id = element["key"]["d_id"];
          key.number = element["key"]["number"];
          
          OrderLine order_line;
          order_line.i_id = element["value"]["i_id"];
          order_line.supply_w_id = element["value"]["supply_w_id"];
          order_line.delivery_d = element["value"]["delivery_d"];
          order_line.quantity = element["value"]["quantity"];
          order_line.amount = element["value"]["amount"];
          order_line.dist_info = element["value"]["dist_info"];

          order_lines_view->put(key, order_line);
          load_count++;
        }

        //LOG_INFO << "done" << std::endl;
        return make_success(load_count);
      };

      auto loadNewOrders = [this](Store::Tx& tx, const nlohmann::json& params)
      {
        //LOG_INFO << "Executing loadNewOrders handler...";

        auto new_orders_view = tx.get_view(tables.neworders);
        int load_count = 0;

        for (auto& element : params)
        {
          NewOrderId key;
          key.o_id = element["key"]["o_id"];
          key.w_id = element["key"]["w_id"];
          key.d_id = element["key"]["d_id"];

          NewOrder new_order;
          new_order.flag = element["value"]["flag"];

          new_orders_view->put(key, new_order);
          load_count++;
        }

        //LOG_INFO << "done" << std::endl;
        return make_success(load_count);
      };

      install(Procs::TPCC_KV_SNAPSHOT, json_adapter(kvSnapshot), HandlerRegistry::Read);
      install(Procs::TPCC_LEDGER_VERIFY, json_adapter(ledgerVerify), HandlerRegistry::Read);
      install(Procs::TPCC_QUERY_HISTORY, json_adapter(queryOrderHistory), HandlerRegistry::Read);
      install(Procs::TPCC_NEW_ORDER, json_adapter(newOrder), HandlerRegistry::Write);
      install(Procs::TPCC_LOAD_ITEMS, json_adapter(loadItems), HandlerRegistry::Write);
      install(Procs::TPCC_LOAD_WAREHOUSE, json_adapter(loadWarehouse), HandlerRegistry::Write);
      install(Procs::TPCC_LOAD_STOCKS, json_adapter(loadStocks), HandlerRegistry::Write);
      install(Procs::TPCC_LOAD_DISTRICT, json_adapter(loadDistrict), HandlerRegistry::Write);
      install(Procs::TPCC_LOAD_CUSTOMER, json_adapter(loadCustomer), HandlerRegistry::Write);
      install(Procs::TPCC_LOAD_HISTORY, json_adapter(loadHistory), HandlerRegistry::Write);
      install(Procs::TPCC_LOAD_ORDER, json_adapter(loadOrder), HandlerRegistry::Write);
      install(Procs::TPCC_LOAD_ORDER_LINES, json_adapter(loadOrderLines), HandlerRegistry::Write);
      install(Procs::TPCC_LOAD_NEW_ORDERS, json_adapter(loadNewOrders), HandlerRegistry::Write);
    }
  };

  class Tpcc : public ccf::UserRpcFrontend
  {
    private:
      TpccHandlers tpcc_handlers;

    public:
      Tpcc(Store& store) :
        UserRpcFrontend(store, tpcc_handlers),
        tpcc_handlers(store) 
      {
        disable_request_storing();
      }
  };
} // namespace tpcc

  std::shared_ptr<ccf::UserRpcFrontend> get_rpc_handler(
    NetworkTables& nwt, AbstractNotifier& notifier)
  {
    return std::make_shared<tpcc::Tpcc>(*nwt.tables);
  }

} // namespace ccfapp
