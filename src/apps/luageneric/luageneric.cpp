// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache 2.0 License.
#include "enclave/appinterface.h"
#include "luainterp/luaargs.h"
#include "luainterp/luainterp.h"
#include "luainterp/luakv.h"
#include "luainterp/txscriptrunner.h"
#include "node/rpc/userfrontend.h"

#include <memory>
#include <vector>

namespace ccfapp
{
  using namespace kv;
  using namespace ccf;
  using namespace lua;

  using GenericTable = ccf::Store::Map<nlohmann::json, nlohmann::json>;

  class AppTsr : public TxScriptRunner
  {
  private:
    void add_error_codes(
      lua_State* l, char const* table_name = "error_codes") const
    {
      // Get env table on top of stack
      lua_getglobal(l, env_table_name);
      if (lua_isnil(l, -1))
      {
        LOG_FAIL_FMT(
          "There is no env table '{}', skipping creation of error codes table "
          "'{}'",
          env_table_name,
          table_name);
      }

      // Create error_codes table
      lua_newtable(l);

#define XX(Name, Value) \
  lua_pushinteger(l, Value); \
  lua_setfield(l, -2, #Name);

      XX_STANDARD_ERROR_CODES;
      XX_CCF_ERROR_CODES;

#undef XX

      lua_setfield(l, -2, table_name);

      // Remove env table from stack
      lua_pop(l, 1);
    }

    void setup_environment(
      lua::Interpreter& li,
      const std::optional<Script>& env_script) const override
    {
      auto l = li.get_state();

      // Create env table
      lua_newtable(l);
      lua_setglobal(l, env_table_name);

      // Register global logging functions
      lua_register(l, "LOG_TRACE", lua_log_trace);
      lua_register(l, "LOG_DEBUG", lua_log_debug);
      lua_register(l, "LOG_INFO", lua_log_info);
      lua_register(l, "LOG_FAIL", lua_log_fail);
      lua_register(l, "LOG_FATAL", lua_log_fatal);

      add_error_codes(li.get_state());

      TxScriptRunner::setup_environment(li, env_script);
    }

    const std::vector<GenericTable*> app_tables;
    void add_custom_tables(
      lua::Interpreter& li,
      ccf::Store::Tx& tx,
      int& n_registered_tables) const override
    {
      n_registered_tables++;
      TableCreator<false>::create(li, tx, app_tables);
    }

  public:
    AppTsr(NetworkTables& network, std::vector<GenericTable*> app_tables) :
      TxScriptRunner(network),
      app_tables(app_tables)
    {}
  };

  class LuaHandlers : public UserHandlerRegistry
  {
  private:
    NetworkTables& network;
    std::unique_ptr<AppTsr> tsr;

  public:
    LuaHandlers(NetworkTables& network, const uint16_t n_tables = 8) :
      UserHandlerRegistry(network),
      network(network)
    {
      auto& tables = *network.tables;

      // create public and private app tables (2x n_tables in total)
      std::vector<GenericTable*> app_tables(n_tables * 2);
      for (uint16_t i = 0; i < n_tables; i++)
      {
        const auto suffix = std::to_string(i);
        app_tables[i] = &tables.create<GenericTable>("priv" + suffix);
        app_tables[i + n_tables] = &tables.create<GenericTable>("pub" + suffix);
      }
      tsr = std::make_unique<AppTsr>(network, app_tables);

      auto default_handler = [this](RequestArgs& args) {
        const auto method = args.rpc_ctx->get_method();
        const auto local_method = method.substr(method.find_first_not_of('/'));
        if (local_method == UserScriptIds::ENV_HANDLER)
        {
          args.rpc_ctx->set_response_error(
            jsonrpc::StandardErrorCodes::METHOD_NOT_FOUND,
            fmt::format("Cannot call environment script ('{}')", local_method));
          return;
        }

        const auto scripts = args.tx.get_view(this->network.app_scripts);

        // try find script for method
        auto handler_script = scripts->get(local_method);
        if (!handler_script)
        {
          args.rpc_ctx->set_response_error(
            jsonrpc::StandardErrorCodes::METHOD_NOT_FOUND,
            fmt::format(
              "No handler script found for method '{}'", local_method));
          return;
        }

        auto response = tsr->run<nlohmann::json>(
          args.tx,
          {*handler_script,
           {},
           WlIds::USER_APP_CAN_READ_ONLY,
           scripts->get(UserScriptIds::ENV_HANDLER)},
          // vvv arguments to the script vvv
          args);

        auto err_it = response.find("error");
        if (err_it == response.end())
        {
          auto result_it = response.find("result");
          if (result_it == response.end())
          {
            // Response contains neither result nor error. It may not even be an
            // object. We assume the entire response is a successful result.
            args.rpc_ctx->set_response_result(std::move(response));
            return;
          }
          else
          {
            args.rpc_ctx->set_response_result(std::move(*result_it));
            return;
          }
        }
        else
        {
          int err_code = jsonrpc::CCFErrorCodes::SCRIPT_ERROR;
          std::string msg = "";

          if (err_it->is_object())
          {
            auto err_code_it = err_it->find("code");
            if (err_code_it != err_it->end())
            {
              err_code = *err_code_it;
            }

            auto err_message_it = err_it->find("message");
            if (err_message_it != err_it->end())
            {
              msg = *err_message_it;
            }
          }

          args.rpc_ctx->set_response_error(err_code, msg);
          return;
        }
      };

      set_default(default_handler, Write);
    }

    // Since we do our own dispatch within the default handler, report the
    // supported methods here
    void list_methods(ccf::Store::Tx& tx, ListMethods::Out& out) override
    {
      UserHandlerRegistry::list_methods(tx, out);

      auto scripts = tx.get_view(this->network.app_scripts);
      scripts->foreach([&out](const auto& key, const auto&) {
        if (key != UserScriptIds::ENV_HANDLER)
        {
          out.methods.push_back(key);
        }
        return true;
      });
    }
  };

  class Lua : public ccf::UserRpcFrontend
  {
  private:
    LuaHandlers lua_handlers;

  public:
    Lua(NetworkTables& network) :
      ccf::UserRpcFrontend(*network.tables, lua_handlers),
      lua_handlers(network)
    {}
  };

  std::shared_ptr<ccf::UserRpcFrontend> get_rpc_handler(
    NetworkTables& network, AbstractNotifier& notifier)
  {
    return std::make_shared<Lua>(network);
  }
} // namespace ccfapp
