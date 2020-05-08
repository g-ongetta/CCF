// Copyright (c) Microsoft Corporation.
// Copyright (c) 1999 Miguel Castro, Barbara Liskov.
// Copyright (c) 2000, 2001 Miguel Castro, Rodrigo Rodrigues, Barbara Liskov.
// Licensed under the MIT license.

#pragma once

#include "ds/logger.h"

extern "C"
{
#include <evercrypt/EverCrypt_Hash.h>
}

class Digest
{
public:
  inline Digest()
  {
    for (int i = 0; i < 4; i++)
      d[i] = 0;
  }
  Digest(char* s, unsigned n)
  {
#ifndef NODIGESTS
    // creates a digest for string "s" with length "n"
    EverCrypt_Hash_hash(
      Spec_Hash_Definitions_SHA2_256, (uint8_t*)d, (uint8_t*)s, (uint32_t)n);

#else
    for (int i = 0; i < 4; i++)
      d[i] = 3;
#endif // NODIGESTS
  }
  // Effects: Creates a digest for string "s" with length "n"

  inline Digest(Digest const& x)
  {
    d[0] = x.d[0];
    d[1] = x.d[1];
    d[2] = x.d[2];
    d[3] = x.d[3];
  }

  inline ~Digest() = default;
  // Effects: Deallocates all storage associated with digest.

  inline void zero()
  {
    for (int i = 0; i < 4; i++)
      d[i] = 0;
  }

  inline bool is_zero() const
  {
    return d[0] == 0;
  }

  inline bool operator==(Digest const& x) const
  {
    return (d[0] == x.d[0]) & (d[1] == x.d[1]) & (d[2] == x.d[2]) &
      (d[3] == x.d[3]);
  }

  inline bool operator==(uint64_t* e) const
  {
    return (d[0] == e[0]) & (d[1] == e[1]) & (d[2] == e[2]) & (d[3] == e[3]);
  }

  inline bool operator!=(Digest const& x) const
  {
    return !(*this == x);
  }

  inline Digest& operator=(Digest const& x)
  {
    d[0] = x.d[0];
    d[1] = x.d[1];
    d[2] = x.d[2];
    d[3] = x.d[3];
    return *this;
  }

  inline size_t hash() const
  {
    return (size_t)d[0];
  }

  char* digest()
  {
    return (char*)d;
  }
  uint64_t* udigest()
  {
    return d;
  }

  constexpr static size_t digest_size()
  {
    return sizeof(d);
  }

  struct Context
  {
    Context() :
      s{(uint32_t)0x6a09e667U,
        (uint32_t)0xbb67ae85U,
        (uint32_t)0x3c6ef372U,
        (uint32_t)0xa54ff53aU,
        (uint32_t)0x510e527fU,
        (uint32_t)0x9b05688cU,
        (uint32_t)0x1f83d9abU,
        (uint32_t)0x5be0cd19U}
    {
      scrut.tag = EverCrypt_Hash_SHA2_256_s;
      scrut.case_SHA2_256_s = s;
    }
    uint32_t s[8U];
    EverCrypt_Hash_state_s scrut;
  };

  // incremental digest computation
  static unsigned block_length()
  {
    // Spec_Hash_Definitions_SHA2_256
    return (uint32_t)64U;
  }
  void update(Digest::Context& ctx, char* s, unsigned n)
  {
    // PBFT_ASSERT(n % block_length() == 0, "n must be a mutiple of
    // block_length()");
    EverCrypt_Hash_update_multi(&ctx.scrut, (uint8_t*)s, n);
  }
  // Requires: n % block_length() == 0
  // Effects: adds the digest of (s,n) to context
  void update_last(Digest::Context& ctx, const char* s, unsigned n)
  {
    EverCrypt_Hash_update_last(&ctx.scrut, (uint8_t*)s, n);
  }
  // Effects: Adds the digest of (s,n) to context with zero padding at the end
  // to the next block_length boundary if n % block_length() != 0
  void finalize(Digest::Context& ctx)
  {
    EverCrypt_Hash_finish(&ctx.scrut, (uint8_t*)d);
  }
  // Effects: finalizes this digest from ctx

  void print()
  {
    LOG_INFO_FMT("digest=[{},{},{},{}]", d[0], d[1], d[2], d[3]);
  }
  // Effects: Prints digest in stdout.

private:
  uint64_t d[4];
};

struct DigestHash
{
  size_t operator()(const Digest& d) const
  {
    return d.hash();
  }
};