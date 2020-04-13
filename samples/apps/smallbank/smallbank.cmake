# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the Apache 2.0 License.
# Small Bank Client executable
add_client_exe(
  small_bank_client
  SRCS ${CMAKE_CURRENT_LIST_DIR}/clients/small_bank_client.cpp
)
target_link_libraries(small_bank_client PRIVATE secp256k1.host http_parser.host)

# SmallBank application
add_ccf_app(smallbank SRCS ${CMAKE_CURRENT_LIST_DIR}/app/smallbank.cpp)
sign_app_library(
  smallbank.enclave ${CMAKE_CURRENT_LIST_DIR}/app/oe_sign.conf
  ${CCF_DIR}/src/apps/sample_key.pem
)

function(get_verification_file iterations output_var)
  math(EXPR thousand_iterations "${iterations} / 1000")
  set(proposed_name
      ${CMAKE_CURRENT_LIST_DIR}/tests/verify_small_bank_${thousand_iterations}k.json
  )
  if(NOT EXISTS "${proposed_name}")
    message(
      FATAL_ERROR
        "Could not find verification file for ${iterations} iterations (looking for ${proposed_name})"
    )
  endif()
  set(${output_var}
      ${proposed_name}
      PARENT_SCOPE
  )
endfunction()

set(SMALL_BANK_SIGNED_ITERATIONS 50000)
get_verification_file(
  ${SMALL_BANK_SIGNED_ITERATIONS} SMALL_BANK_SIGNED_VERIFICATION_FILE
)

if(BUILD_TESTS)
  # Small Bank end to end and performance test
  foreach(CONSENSUS ${CONSENSUSES})

    if(${CONSENSUS} STREQUAL pbft)
      if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(SMALL_BANK_ITERATIONS 50000)
      else()
        set(SMALL_BANK_ITERATIONS 2000)
      endif()
    else()
      set(SMALL_BANK_ITERATIONS 200000)
    endif()
    get_verification_file(${SMALL_BANK_ITERATIONS} SMALL_BANK_VERIFICATION_FILE)

    add_perf_test(
      NAME small_bank_client_test_${CONSENSUS}
      PYTHON_SCRIPT ${CMAKE_CURRENT_LIST_DIR}/tests/small_bank_client.py
      CLIENT_BIN ./small_bank_client
      VERIFICATION_FILE ${SMALL_BANK_VERIFICATION_FILE}
      LABEL SB
      CONSENSUS ${CONSENSUS}
      ADDITIONAL_ARGS
        --transactions ${SMALL_BANK_ITERATIONS} --max-writes-ahead 1000
        --metrics-file small_bank_${CONSENSUS}_metrics.json
    )

    add_perf_test(
      NAME small_bank_sigs_client_test_${CONSENSUS}
      PYTHON_SCRIPT ${CMAKE_CURRENT_LIST_DIR}/tests/small_bank_client.py
      CLIENT_BIN ./small_bank_client
      VERIFICATION_FILE ${SMALL_BANK_SIGNED_VERIFICATION_FILE}
      LABEL "SB_sig"
      CONSENSUS ${CONSENSUS}
      ADDITIONAL_ARGS
        --transactions
        ${SMALL_BANK_SIGNED_ITERATIONS}
        --max-writes-ahead
        1000
        --sign
        --participants-curve
        "secp256k1"
        --metrics-file
        small_bank_${CONSENSUS}_sigs_metrics.json
    )

  endforeach()

  # It is better to run performance tests with forwarding on different machines
  # (i.e. nodes and clients)
  add_perf_test(
    NAME small_bank_sigs_forwarding
    PYTHON_SCRIPT ${CMAKE_CURRENT_LIST_DIR}/tests/small_bank_client.py
    CLIENT_BIN ./small_bank_client
    LABEL "SB_sig_fwd"
    CONSENSUS raft
    ADDITIONAL_ARGS
      --transactions
      ${SMALL_BANK_SIGNED_ITERATIONS}
      --max-writes-ahead
      1000
      --metrics-file
      small_bank_fwd_metrics.json
      --send-tx-to
      backups
      --sign
      --participants-curve
      "secp256k1"
      --one-client-per-backup
  )

endif()
