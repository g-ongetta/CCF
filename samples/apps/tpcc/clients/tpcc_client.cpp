// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#include "perf_client.h"
#include <chrono>

using namespace std;
using namespace nlohmann;

struct TpccClientOptions : public client::PerfOptions
{
  uint64_t num_warehouses = 3; 
  std::string query_method = "none"; // [none|kv|ledger]

  TpccClientOptions(CLI::App& app, const std::string& default_pid_file) :
    client::PerfOptions("Tpcc_ClientCpp", default_pid_file, app)
  {
    app.add_option("--warehouses", num_warehouses)->capture_default_str();
    app.add_option("--query-method", query_method)->capture_default_str();
  }
};

using Base = client::PerfBase<TpccClientOptions>;

class TpccClient : public Base
{
private:

  // Set the date of 'history' entries to sequential timestamps
  bool set_history_date = true;
  
  // TPCC constants
  const uint64_t num_districts = 10;   // 10 in spec
  const uint64_t num_customers = 3000; // 3000 in spec
  const uint64_t num_orders = 3000;    // 3000 in spec
  const uint64_t num_new_orders = 900; // 900 in spec
  const uint64_t num_items = 1000;     // 100000 in spec
  const uint64_t num_stocks = 1000;    // 100000 in spec


  std::optional<RpcTlsClient::Response> send_creation_transactions() override
  {
    LOG_INFO << "Sending Data Generation Transactions..." << endl;

    // Load the Items table
    load_items();
    LOG_INFO << "Loaded Items" << endl;
    
    LOG_INFO << "Loading Warehouses..." << endl;
    // Load the Warehouses, for each warehouse, load districts and stocks
    for (uint64_t w_id = 1; w_id <= options.num_warehouses; w_id++)
    {
      load_warehouse(w_id);
      load_stocks(w_id);

      // Load districts, for each district, load customers and orders
      for (uint64_t d_id = 1; d_id <= num_districts; d_id++)
      {
        load_district(d_id, w_id);

        // Find customer IDs with bad credit (10%)
        std::unordered_set<uint64_t> bad_credit_ids = select_n_unique(num_customers / 10, 1, num_customers);

        // Load customers. For each customer, load history
        for (uint64_t c_id = 1; c_id <= num_customers; c_id++)
        {
          bool bad_credit = bad_credit_ids.find(c_id) != bad_credit_ids.end();

          load_customer(c_id, d_id, w_id, bad_credit);
          load_history(c_id, d_id, w_id);
        }

        // Create random permutation for customer IDs
        int* c_id_perms = permutation(1, num_orders);

        // Load orders. For each order, load order line
        for (uint64_t o_id = 1; o_id <= num_orders; o_id++)
        {
          uint64_t o_ol_cnt = rand_range(5, 15);

          load_order(o_id, o_ol_cnt, d_id, w_id, c_id_perms[o_id]);
          load_order_lines(o_id, o_ol_cnt, d_id, w_id);
        }

        // Load new orders for the last 900 order Ids
        load_new_orders(num_orders - num_new_orders, num_orders, d_id, w_id);

        LOG_INFO_FMT("Loaded District {}/{}", d_id, num_districts);
      }

      LOG_INFO_FMT("Loaded Warehouse {}/{}", w_id, options.num_warehouses);
    }

    return {};
  }

  void prepare_transactions() override
  {
    // Reserve space for transactions
    prepared_txs.resize(options.num_transactions);

    // If query method is 'none', prepare NewOrder transactions, otherwise prepare
    // history query transactions
    for (decltype(options.num_transactions) i = 0; i < options.num_transactions; i++)
    {
      if (options.query_method == "none")
      {
        json params = generate_new_order_params();
        add_prepared_tx("TPCC_new_order", params, true, i);
      }
      else if (options.query_method == "snapshot")
      {
        add_prepared_tx("TPCC_kv_snapshot", {}, true, i);
      }
      else
      {
        json query_params = generate_query_history_params(options.query_method);
        add_prepared_tx("TPCC_query_history", query_params, true, i);
      }
    }
  }

  bool check_response(const RpcTlsClient::Response& r) override {
    if (r.status == HTTP_STATUS_OK)
    {
      const json body = jsonrpc::unpack(r.body, jsonrpc::Pack::MsgPack);
      LOG_INFO_FMT("RESPONSE: {}", body);
      return true;
    }

    const std::string error_msg(r.body.begin(), r.body.end());
    if (error_msg.find("Item Not Found") == std::string::npos)
    {
      throw logic_error(error_msg);
      return false;
    }

    return true;
  }

  json generate_new_order_params()
  {
    json params;

    // Warehouse ID
    uint64_t w_id = rand_range(options.num_warehouses) + 1;
    params["w_id"] = w_id;

    // District ID: Rand[1, 10] from home warehouse
    params["d_id"] = rand_range(1, (int) num_districts + 1);

    // Customer ID: NURand[1023, 1, 3000] from district number
    params["c_id"] = nu_rand(35, 1, (int) num_customers + 1);

    // Entry Date: current date time
    std::time_t t = curr_date();
    params["o_entry_d"] = date_str(t);

    // Number of items: Rand[5, 15]
    uint64_t ol_cnt = rand_range(5, 16);
    
    params["i_ids"] = {};
    params["i_w_ids"] = {};
    params["i_qtys"] = {};

    // 1% of transactions will rollback
    bool rollback = rand_range(0, 100) == 0;

    // Generate Items
    for (size_t i = 1; i <= ol_cnt; i++)
    {
      // Item Id: NURand[8191, 1, 100000]
      uint64_t i_id = nu_rand(82, 1, (int) num_items + 1);

      if (rollback && i == ol_cnt)
      {
        i_id = num_items + 1; // Unused value
      }

      params["i_ids"].push_back(i_id);

      // Supplying Warehouse: 99% home, 1% remote
      uint64_t o_supply_w_id = w_id;
      if (rand_range(0, 100) == 0)
      {
        do
        {
          o_supply_w_id = rand_range(options.num_warehouses) + 1;
        }
        while (o_supply_w_id == w_id);
      }

      params["i_w_ids"].push_back(o_supply_w_id);

      // Quantity: Rand[1, 10]
      params["i_qtys"].push_back(rand_range(1, 11));
    }

    return params;
  }

  json generate_query_history_params(std::string query_method) {
    json params;

    static const uint64_t num_history = options.num_warehouses * num_districts * num_customers;

    // Generate earlier 'from' date (between 0 and num_history hours in the past)
    uint64_t num_hours_from = rand_range(0ul, num_history + 1);
    std::time_t date_from = past_date(num_hours_from);

    // Generate later 'to' date (between 0 and num_hours hours in the past)
    uint64_t num_hours_to = rand_range(0ul, num_hours_from + 1);
    std::time_t date_to = past_date(num_hours_to);

    params["date_from"] = date_str(date_from);
    params["date_to"] = date_str(date_to);
    params["method"] = query_method;
    return params;
  }

  /* ----- Data loading methods ----- */

  void load_items()
  {
    std::unordered_set<uint64_t> original_rows = select_n_unique(num_items / 10, 1, num_items);

    std::vector<json> items_array;
    items_array.reserve(num_items);

    for (uint64_t i = 1; i <= num_items; i++)
    {
      bool is_original = original_rows.find(i) != original_rows.end();
      
      json item;
      item["key"] = i;
      item["value"] = make_item(is_original);
      items_array.push_back(item);
    }

    auto connection = get_connection();
    auto response = connection->call("TPCC_load_items", items_array);
    handle_load_response(response, "TPCC_load_items");
  }

  void load_warehouse(std::uint64_t w_id)
  {
    // Load the warehouse entry
    json warehouse;
    warehouse["key"] = w_id;
    warehouse["value"] = make_warehouse();

    auto connection = get_connection();
    auto response = connection->call("TPCC_load_warehouse", warehouse);
    handle_load_response(response, "TPCC_load_warehouse");
  }

  void load_stocks(std::uint64_t w_id)
  {
    std::unordered_set<uint64_t> original_rows = select_n_unique(num_stocks / 10, 1, num_stocks);

    std::vector<json> stocks_array;
    stocks_array.reserve(num_stocks);

    for (uint64_t i = 1; i <= num_stocks; i++)
    {
      json key;
      key["i_id"] = i;
      key["w_id"] = w_id;

      bool is_original = original_rows.find(i) != original_rows.end();

      json stock;
      stock["key"] = key;
      stock["value"] = make_stock(is_original);
      stocks_array.push_back(stock);
    }

    auto connection = get_connection();
    auto response = connection->call("TPCC_load_stocks", stocks_array);
    handle_load_response(response, "TPCC_load_stocks");
  }

  void load_district(uint64_t d_id, uint64_t w_id)
  {
    json key;
    key["id"] = d_id;
    key["w_id"] = w_id;

    json district;
    district["key"] = key;
    district["value"] = make_district();

    auto connection = get_connection();
    auto response = connection->call("TPCC_load_district", district);
    handle_load_response(response, "TPCC_load_district");
  }
  
  void load_customer(uint64_t c_id, uint64_t d_id, uint64_t w_id, bool bad_credit)
  {
    json key;
    key["id"] = c_id;
    key["w_id"] = w_id;
    key["d_id"] = d_id;

    json customer;
    customer["key"] = key;
    customer["value"] = make_customer(c_id, bad_credit);

    auto connection = get_connection();
    auto response = connection->call("TPCC_load_customer", customer);
    handle_load_response(response, "TPCC_load_customer");
  }

  void load_history(uint64_t c_id, uint64_t d_id, uint64_t w_id)
  {
    json history;
    history["key"] = c_id; // Using c_id as an incrementing ID
    history["value"] = make_history(c_id, d_id, w_id);

    auto connection = get_connection();
    auto response = connection->call("TPCC_load_history", history);
    handle_load_response(response, "TPCC_load_history");
  }

  void load_order(uint64_t o_id, uint64_t o_ol_cnt, uint64_t d_id, uint64_t w_id, uint64_t c_id)
  {
    json key;
    key["id"] = o_id;
    key["d_id"] = d_id;
    key["w_id"] = w_id;

    json order;
    order["key"] = key;
    order["value"] = make_order(o_ol_cnt, c_id, o_id >= 2101);

    auto connection = get_connection();
    auto response = connection->call("TPCC_load_order", order);
    handle_load_response(response, "TPCC_load_order");
  }

  void load_order_lines(uint64_t o_id, uint64_t o_ol_cnt, uint64_t d_id, uint64_t w_id)
  {
    std::vector<json> order_lines_array;
    order_lines_array.reserve(o_ol_cnt);

    for (size_t i = 1; i <= o_ol_cnt; i++) {
      json key;
      key["o_id"] = o_id;
      key["d_id"] = d_id;
      key["w_id"] = w_id;
      key["number"] = i;

      json order_line;
      order_line["key"] = key;
      order_line["value"] = make_order_line(w_id, o_id >= 2101, o_id < 2101);
      order_lines_array.push_back(order_line);
    }

    auto connection = get_connection();
    auto response = connection->call("TPCC_load_order_lines", order_lines_array);
    handle_load_response(response, "TPCC_load_order_lines");
  }

  void load_new_orders(uint64_t start, uint64_t end, uint64_t d_id, uint64_t w_id)
  {
    uint64_t amount = end - start + 1;

    std::vector<json> new_orders_array;
    new_orders_array.reserve(amount);

    for (size_t i = start; i <= end; i++) {
      json key;
      key["o_id"] = i;
      key["d_id"] = d_id;
      key["w_id"] = w_id;

      json value;
      value["flag"] = 0;

      json new_order;
      new_order["key"] = key;
      new_order["value"] = value;

      new_orders_array.push_back(new_order);
    }

    auto connection = get_connection();
    auto response = connection->call("TPCC_load_new_orders", new_orders_array);
    handle_load_response(response, "TPCC_load_new_orders");
  }

  /* Individual tuple generators */

  json make_item(bool is_original)
  {
    json item;
    item["im_id"] = rand_range(1, 10000);
    item["price"] = rand_range(100, 10000) / 100;
    item["name"] = rand_astring(14, 24);
    item["data"] = is_original
      ? rand_insert(rand_astring(26, 50), "ORIGINAL")
      : rand_astring(26, 50);
    return item;
  }

  json make_warehouse()
  {
    json warehouse;
    warehouse["name"] = rand_astring(6, 10);
    warehouse["street_1"] = rand_astring(10, 20);
    warehouse["street_2"] = rand_astring(10, 20);
    warehouse["city"] = rand_astring(10, 20);
    warehouse["state"] = rand_astring(2, 2);
    warehouse["zip"] = make_zipcode();
    warehouse["tax"] = rand_range(0, 2000 + 1) / 1000;
    warehouse["ytd"] = 0;
    return warehouse;
  }

  json make_stock(bool is_original)
  {
    json stock;
    stock["quantity"] = rand_range(10, 100 + 1);
    stock["ytd"] = 0;
    stock["order_cnt"] = 0;
    stock["remote_cnt"] = 0;
    stock["data"] = is_original
      ? rand_insert(rand_astring(26, 50), "ORIGINAL")
      : rand_astring(26, 50);

    json dist_xx[10];
    for (size_t i = 0; i < 10; i++)
    {
      dist_xx[i] = rand_astring(24, 24);
    }

    stock["dist_xx"] = dist_xx;
    return stock;
  }

  json make_district()
  {
    json district;
    district["name"] = rand_astring(6, 10);
    district["street_1"] = rand_astring(10, 20);
    district["street_2"] = rand_astring(10, 20);
    district["city"] = rand_astring(10, 20);
    district["state"] = rand_astring(2, 2);
    district["zip"] = make_zipcode();
    district["tax"] = (double) rand_range(0, 2000) / 1000;
    district["ytd"] = 0;
    district["next_o_id"] = 3001;
    return district;
  }

  json make_customer(uint64_t c_id, bool bad_credit)
  {
    json customer;
    customer["last"] = make_customer_last(c_id);
    customer["middle"] = "OE";
    customer["first"] = rand_astring(8, 16);
    customer["street_1"] = rand_astring(10, 20);
    customer["street_2"] = rand_astring(10, 20);
    customer["city"] = rand_astring(10, 20);
    customer["state"] = rand_astring(2, 2);
    customer["zip"] = make_zipcode();
    customer["phone"] = rand_nstring(16, 16);
    customer["since"] = date_str(curr_date());
    customer["credit"] = bad_credit ? "BC" : "GC";
    customer["credit_lim"] = 50000.00;
    customer["discount"] = rand_range(0, 5001) / 1000;
    customer["balance"] = -10.00;
    customer["ytd_payment"] = 10.00;
    customer["payment_cnt"] = 1;
    customer["delivery_cnt"] = 0;
    customer["data"] = rand_astring(300, 500);
    return customer;
  }

  json make_customer_last(uint64_t c_id)
  {
    static const std::vector<std::string> syllables = {"BAR", "OUGHT", "ABLE",
      "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

    uint64_t selection = c_id < 1000 ? c_id : nu_rand(255, 0, 999);

    std::stringstream ss;
    ss << std::setw(4) << std::setfill('0') << selection;
    std::string selection_str = ss.str();

    std::string result = "";

    for (int i = 0; i < 4; i++)
    {
      result += syllables[(int)selection_str[i] - 48];
    }

    return result;
  }

  json make_history(uint64_t c_id, uint64_t d_id, uint64_t w_id)
  {
    static uint64_t num_history = options.num_warehouses * num_districts * num_customers;

    // Generate time stamp for entry
    std::time_t t = set_history_date
      ? past_date(num_history--)
      : std::time(0);

    json history;
    history["c_id"] = c_id;
    history["c_d_id"] = d_id;
    history["c_w_id"] = w_id;
    history["d_id"] = d_id;
    history["w_id"] = w_id;
    history["date"] = date_str(t);
    history["amount"] = 10.0;
    history["data"] = rand_astring(12, 24); 

    return history;
  }

  json make_order(uint64_t o_ol_cnt, uint64_t c_id, bool null_carrier)
  {
    // Current time
    std::time_t t = curr_date();

    json order;
    order["c_id"] = c_id;
    order["entry_d"] = date_str(t);
    order["carrier_id"] = null_carrier ? -1 : rand_range(1, 11);
    order["ol_cnt"] = o_ol_cnt;
    order["all_local"] = 1;
    return order;
  }

  json make_order_line(uint64_t w_id, bool null_delivery_d, bool null_amount)
  {
    // Current time
    std::time_t t = curr_date();

    json order_line;
    order_line["i_id"] = rand_range(1, 100001);
    order_line["supply_w_id"] = w_id;
    order_line["delivery_d"] = null_delivery_d ? "" : date_str(t);
    order_line["quantity"] = 5;
    order_line["amount"] = null_amount ? 0.0 : rand_range(1, 999999) / 100;
    order_line["dist_info"] = rand_astring(24, 24);
    return order_line;
  }

  std::string make_zipcode()
  {
    return rand_nstring(4, 4) + "11111";
  }

  void handle_load_response(HttpRpcTlsClient::Response response, std::string rpc_endpoint) {
    auto connection = get_connection();

    if (response.status != HTTP_STATUS_OK) {
      auto response_body = connection->unpack_body(response);
      throw std::runtime_error("[" + rpc_endpoint + "] Response Error: " + response_body.dump());
    }
  }

  /* ----- Random Utils ----- */

  /*
    Non-Uniform Random number, NURand[A, x, y], as per TPCC 2.1.6
  */
  template <typename T>
  T nu_rand(T a, T x, T y)
  {
    T c = 0;
    // TODO: implement setting C correctly
    return (((rand_range(0, a) | rand_range(x, y)) + c) % (y - x + 1)) + x;
  }

  /*
    Select n unique numbers from the range [min, max]
  */
  std::unordered_set<uint64_t> select_n_unique(size_t n, uint64_t min, uint64_t max)
  {
    std::unordered_set<uint64_t> unique;

    for (size_t i = 0; i < n; i++)
    {
      uint64_t item;
      do
      {
        item = rand_range(min, max + 1);
      }
      while (unique.find(item) != unique.end());

      unique.emplace(item);
    }

    return unique;
  }

  /*
    Random a-string generator (alphanumeric)
  */
  std::string rand_astring(size_t min_len, size_t max_len)
  {
    constexpr char charset[] =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    return rand_string(min_len, max_len, charset, sizeof(charset));
  }

  /*
    Random n-string generator (numeric)
  */
  std::string rand_nstring(size_t min_len, size_t max_len)
  {
    constexpr char charset[] = "0123456789";

    return rand_string(min_len, max_len, charset, sizeof(charset));
  }

  std::string rand_string(int min_len, int max_len, const char* charset, int charset_len)
  {
    int length = rand_range(min_len, max_len + 1);
    std::string result = "";

    for (int i = 0; i < length; i++)
    {
      int index = rand_range(0, charset_len);
      result += charset[index];
    }

    return result;
  }

  /*
    Places the given substring in a random position in the input
  */
  std::string rand_insert(std::string input, std::string substring)
  {
    int max_index = input.length() - substring.length();
    int start_index = rand_range(0, max_index);

    return input.replace(start_index, substring.length(), substring);
  }

  /*
    Returns array with a randomized sequence of numbers in the range min to max
  */
  int* permutation(uint64_t min, uint64_t max)
  {
    int size = max - min + 1;
    int* results = new int[size]{};

    for (int i = 0; i < size; i++)
    {
      int index;
      do
      {
        index = rand_range(0, size);
      } while (results[index] != 0);

      results[index] = i;
    }

    return results;
  }

  /*
    Return a string for the current date
  */
  std::string date_str(std::time_t t)
  {
    std::stringstream ss;
    ss << std::put_time(std::localtime(&t), "%F %T");
    return ss.str();
  }

  std::time_t curr_date()
  {
    // Current time clock
    auto now = std::chrono::system_clock::now();

    // Return time object
    return std::chrono::system_clock::to_time_t(now);
  }

  /*
    Returns a random date in the past, within a given number of hours
  */
  std::time_t rand_date(uint64_t range_hours)
  {
    return past_date(rand_range(0ul, range_hours + 1));
  }

  /*
    Returns the date in the past given by the number of hours
  */
  std::time_t past_date(uint64_t num_hours)
  {
    // Current time clock
    auto now = std::chrono::system_clock::now();

    // Return time offset by num_hours
    return std::chrono::system_clock::to_time_t(now - std::chrono::hours(num_hours));
  }

public:
  TpccClient(const TpccClientOptions& o) : Base(o) {}
};

int main(int argc, char** argv)
{
  CLI::App cli_app{"TPCC Client"};
  TpccClientOptions options(cli_app, argv[0]);
  CLI11_PARSE(cli_app, argc, argv);

  TpccClient client(options);
  client.run();

  return 0;
}