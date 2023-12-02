
#pragma once

#include <dcc/common/ClassOf.h>
#include <dcc/common/FixedString.h>
#include <dcc/common/Hash.h>
#include <dcc/common/Serialization.h>
#include <dcc/core/SchemaDef.h>


namespace dcc {
namespace evm {
static constexpr auto __BASE_COUNTER__ = __COUNTER__ + 1;
static constexpr auto EVM_FIELD_SIZE = 10;
} // namespace evm
} // namespace dcc  

#undef NAMESPACE_FIELDS
#define NAMESPACE_FIELDS(x) x(dcc) x(evm)

#define EVM_KEY_FIELDS(x, y) x(int32_t, Y_KEY)
#define EVM_VALUE_FIELDS(x, y)                                                \
  x(FixedString<EVM_FIELD_SIZE>, Y_F01)                                       \
      y(FixedString<EVM_FIELD_SIZE>, Y_F02)                                   \
          y(FixedString<EVM_FIELD_SIZE>, Y_F03)                               \
              y(FixedString<EVM_FIELD_SIZE>, Y_F04)                           \
                  y(FixedString<EVM_FIELD_SIZE>, Y_F05)                       \
                      y(FixedString<EVM_FIELD_SIZE>, Y_F06)                   \
                          y(FixedString<EVM_FIELD_SIZE>, Y_F07)               \
                              y(FixedString<EVM_FIELD_SIZE>, Y_F08)           \
                                  y(FixedString<EVM_FIELD_SIZE>, Y_F09)       \
                                      y(FixedString<EVM_FIELD_SIZE>, Y_F10)

DO_STRUCT(evm, EVM_KEY_FIELDS, EVM_VALUE_FIELDS, NAMESPACE_FIELDS)

namespace dcc {

template <> class Serializer<evm::evm::value> {
public:
  std::string operator()(const evm::evm::value &v) {
    return Serializer<decltype(v.Y_F01)>()(v.Y_F01) +
           Serializer<decltype(v.Y_F02)>()(v.Y_F02) +
           Serializer<decltype(v.Y_F03)>()(v.Y_F03) +
           Serializer<decltype(v.Y_F04)>()(v.Y_F04) +
           Serializer<decltype(v.Y_F05)>()(v.Y_F05) +
           Serializer<decltype(v.Y_F06)>()(v.Y_F06) +
           Serializer<decltype(v.Y_F07)>()(v.Y_F07) +
           Serializer<decltype(v.Y_F08)>()(v.Y_F08) +
           Serializer<decltype(v.Y_F09)>()(v.Y_F09) +
           Serializer<decltype(v.Y_F10)>()(v.Y_F10);
  }
};

template <> class Deserializer<evm::evm::value> {
public:
  std::size_t operator()(StringPiece str, evm::evm::value &result) const {

    std::size_t sz = Deserializer<decltype(result.Y_F01)>()(str, result.Y_F01);
    str.remove_prefix(sz);
    Deserializer<decltype(result.Y_F02)>()(str, result.Y_F02);
    str.remove_prefix(sz);
    Deserializer<decltype(result.Y_F03)>()(str, result.Y_F03);
    str.remove_prefix(sz);
    Deserializer<decltype(result.Y_F04)>()(str, result.Y_F04);
    str.remove_prefix(sz);
    Deserializer<decltype(result.Y_F05)>()(str, result.Y_F05);
    str.remove_prefix(sz);
    Deserializer<decltype(result.Y_F06)>()(str, result.Y_F06);
    str.remove_prefix(sz);
    Deserializer<decltype(result.Y_F07)>()(str, result.Y_F07);
    str.remove_prefix(sz);
    Deserializer<decltype(result.Y_F08)>()(str, result.Y_F08);
    str.remove_prefix(sz);
    Deserializer<decltype(result.Y_F09)>()(str, result.Y_F09);
    str.remove_prefix(sz);
    Deserializer<decltype(result.Y_F10)>()(str, result.Y_F10);
    str.remove_prefix(sz);
    return sz * 10;
  }
};

template <> class ClassOf<evm::evm::value> {
public:
  static constexpr std::size_t size() {
    return ClassOf<decltype(evm::evm::value::Y_F01)>::size() * 10;
  }
};

} // namespace dcc  