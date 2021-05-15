#include "storage/index/generic_key.h"

namespace noisepage::storage::index {

/**
 * @param type_id TypeId to interpret both pointers as
 * @param lhs_attr first value to be compared
 * @param rhs_attr second value to be compared
 * @return true if first is less than second
 */
bool GenericKey::CompareLessThan(const type::TypeId type_id, const byte *const lhs_attr, const byte *const rhs_attr) const {
  auto *metadata = metadata_;
  return false;
}

/**
 * @param type_id TypeId to interpret both pointers as
 * @param lhs_attr first value to be compared
 * @param rhs_attr second value to be compared
 * @return true if first is greater than second
 */
bool GenericKey::CompareGreaterThan(const type::TypeId type_id, const byte *const lhs_attr, const byte *const rhs_attr) const {
  return false;
}

/**
 * @param type_id TypeId to interpret both pointers as
 * @param lhs_attr first value to be compared
 * @param rhs_attr second value to be compared
 * @return true if first is equal to second
 */
bool GenericKey::CompareEquals(const type::TypeId type_id, const byte *const lhs_attr, const byte *const rhs_attr) const {
  return false;
}

}  // namespace noisepage::storage::index
