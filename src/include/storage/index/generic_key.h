#pragma once

#include <algorithm>
#include <cstring>
#include <functional>
#include <vector>

#include "common/hash_util.h"
#include "storage/index/index_metadata.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"
#include "xxHash/xxh3.h"

namespace noisepage::storage::index {

// This is the maximum number of bytes to pack into a single GenericKey template. This constraint is arbitrary and can
// be increased if 512 bytes is too small for future workloads.
constexpr uint16_t GENERICKEY_MAX_SIZE = 512;

/**
 * GenericKey is a slower key type than CompactIntsKey for use when the constraints of CompactIntsKey make it
 * unsuitable. For example, GenericKey supports VARLEN and NULLable attributes.
 */

class GenericKey {
 public:
  /**
   * Set the GenericKey's data based on a ProjectedRow and associated index metadata
   * @param from ProjectedRow to generate GenericKey representation of
   * @param metadata index information, key_schema used to interpret PR data correctly
   * @param num_attrs Number of attributes
   */
  void SetFromProjectedRow(const storage::ProjectedRow &from, const IndexMetadata &metadata, size_t num_attrs) {
    NOISEPAGE_ASSERT(from.NumColumns() == metadata.GetSchema().GetColumns().size(),
                     "ProjectedRow should have the same number of columns at the original key schema.");
    metadata_ = &metadata;
    uint64_t total_size = metadata.KeySize();
    NOISEPAGE_ASSERT(total_size <= 512, "Total key size is more than 512 bytes.");

    if (total_size <= 64) {
      data_size_ = 64;
    } else if (total_size <= 128) {
      data_size_ = 128;
    } else if (total_size <= 256) {
      data_size_ = 256;
    } else {
      total_size = GENERICKEY_MAX_SIZE;
    }

    if (key_data_ != nullptr) {
      delete key_data_;
    }
    key_data_ = new byte[total_size];
    std::memset(key_data_, 0, total_size);

    if (metadata.MustInlineVarlen()) {
      const auto &key_schema = metadata.GetSchema();
      const auto &inlined_attr_sizes = metadata.GetInlinedAttributeSizes();

      const ProjectedRowInitializer &generic_key_initializer = metadata.GetInlinedPRInitializer();

      auto *const pr = GetProjectedRow();
      generic_key_initializer.InitializeRow(pr);

      UNUSED_ATTRIBUTE const auto &key_cols = key_schema.GetColumns();
      NOISEPAGE_ASSERT(num_attrs > 0 && num_attrs <= key_cols.size(), "Number of attributes violates invariant");

      for (uint16_t i = 0; i < num_attrs; i++) {
        const auto offset = from.ColumnIds()[i].UnderlyingValue();
        NOISEPAGE_ASSERT(offset == pr->ColumnIds()[i].UnderlyingValue(), "PRs must have the same comparison order!");
        const byte *const from_attr = from.AccessWithNullCheck(offset);
        if (from_attr == nullptr) {
          pr->SetNull(offset);
        } else {
          const auto inlined_attr_size = inlined_attr_sizes[i];
          // TODO(Gus): Magic number
          if (inlined_attr_size <= 16) {
            std::memcpy(pr->AccessForceNotNull(offset), from_attr, inlined_attr_sizes[i]);
          } else {
            // Convert the VarlenEntry to be inlined
            const auto varlen = *reinterpret_cast<const VarlenEntry *const>(from_attr);
            byte *const to_attr = pr->AccessForceNotNull(offset);
            const auto varlen_size = varlen.Size();
            *reinterpret_cast<uint32_t *const>(to_attr) = varlen_size;
            NOISEPAGE_ASSERT(reinterpret_cast<uintptr_t>(to_attr) + sizeof(uint32_t) + varlen_size <=
                             reinterpret_cast<uintptr_t>(this) + data_size_,
                             "ProjectedRow will access out of bounds.");
            std::memcpy(to_attr + sizeof(uint32_t), varlen.Content(), varlen_size);
          }
        }
      }
    } else {
      NOISEPAGE_ASSERT(
          reinterpret_cast<uintptr_t>(GetProjectedRow()) + from.Size() <= reinterpret_cast<uintptr_t>(this) + data_size_,
          "ProjectedRow will access out of bounds.");
      // We recast GetProjectedRow() as a workaround for -Wclass-memaccess
      std::memcpy(static_cast<void *>(GetProjectedRow()), &from, from.Size());
    }
  }

  /**
   * @return Aligned pointer to the key's internal ProjectedRow, exposed for hasher and comparators
   */
  const ProjectedRow *GetProjectedRow() const {
    const auto *pr = reinterpret_cast<const ProjectedRow *>(StorageUtil::AlignedPtr(sizeof(uint64_t), key_data_));
    NOISEPAGE_ASSERT(reinterpret_cast<uintptr_t>(pr) % sizeof(uint64_t) == 0,
                     "ProjectedRow must be aligned to 8 bytes for atomicity guarantees.");
    NOISEPAGE_ASSERT(reinterpret_cast<uintptr_t>(pr) + pr->Size() <= reinterpret_cast<uintptr_t>(this) + data_size_,
                     "ProjectedRow will access out of bounds.");
    return pr;
  }

  /**
   * @return metadata of the index for this key, exposed for hasher and comparators
   */
  const IndexMetadata &GetIndexMetadata() const {
    NOISEPAGE_ASSERT(metadata_ != nullptr, "This key has no metadata.");
    return *metadata_;
  }

  /**
   * Utility class to evaluate comparisons of embedded types within a ProjectedRow. This is not exposed somewhere like
   * type/type_util.h becauase these do not enforce SQL comparison semantics (i.e. NULL comparisons evaluate to NULL).
   * These comparisons are merely for ordering semantics. If it makes sense for this to be moved somewhere more general
   * purpose in the future, feel free to do so, but for now the only use is in GenericKey and I don't want to encourage
   * their misuse elsewhere in the system.
   */
  struct TypeComparators {
    TypeComparators() = delete;

    /**
     * @param lhs_attr first VarlenEntry to be compared
     * @param rhs_attr second VarlenEntry to be compared
     * @return std::memcmp semantics: < 0 means first is less than second, 0 means equal, > 0 means first is greater
     * than second
     */
    static int CompareVarlens(const byte *const lhs_attr, const byte *const rhs_attr) {
      const uint32_t lhs_size = *reinterpret_cast<const uint32_t *const>(lhs_attr);
      const uint32_t rhs_size = *reinterpret_cast<const uint32_t *const>(rhs_attr);
      const auto smallest_size = std::min(lhs_size, rhs_size);

      // get the pointers to the content
      const byte *const lhs_content = lhs_attr + sizeof(uint32_t);
      const byte *const rhs_content = rhs_attr + sizeof(uint32_t);
      auto result = std::memcmp(lhs_content, rhs_content, smallest_size);
      if (result == 0 && lhs_size != rhs_size) {
        // strings compared as equal, but they have different lengths. Decide based on length
        result = lhs_size - rhs_size;
      }
      return result;
    }

  };

  /**
   * Returns whether this key is less than another key up to num_attrs for comparison.
   * @param rhs other key to compare against
   * @param metadata IndexMetadata
   * @param num_attrs attributes to compare against
   * @returns whether this is less than other
   */
  bool PartialLessThan(const GenericKey &rhs, UNUSED_ATTRIBUTE const IndexMetadata *metadata,
                       size_t num_attrs) const {
    const auto &key_schema = GetIndexMetadata().GetSchema();
    UNUSED_ATTRIBUTE const auto &key_cols = key_schema.GetColumns();
    NOISEPAGE_ASSERT(num_attrs > 0 && num_attrs <= key_cols.size(), "Invalid num_attrs for generic key");

    for (uint16_t i = 0; i < num_attrs; i++) {
      const auto *const lhs_pr = GetProjectedRow();
      const auto *const rhs_pr = rhs.GetProjectedRow();

      const auto offset = lhs_pr->ColumnIds()[i].UnderlyingValue();
      NOISEPAGE_ASSERT(lhs_pr->ColumnIds()[i] == rhs_pr->ColumnIds()[i], "Comparison orders should be the same.");

      const byte *const lhs_attr = lhs_pr->AccessWithNullCheck(offset);
      const byte *const rhs_attr = rhs_pr->AccessWithNullCheck(offset);

      if (lhs_attr == nullptr) {
        if (rhs_attr == nullptr) {
          // attributes are both NULL (equal), continue
          continue;
        }
        // lhs is NULL, rhs is non-NULL, lhs is less than
        return true;
      }

      if (rhs_attr == nullptr) {
        // lhs is non-NULL, rhs is NULL, lhs is greater than
        return false;
      }

      const noisepage::type::TypeId type_id = key_schema.GetColumns()[i].Type();
      const uint16_t attribute_size = key_schema.GetColumns()[i].AttributeLength();
      if (type_id == noisepage::type::TypeId::VARCHAR || type_id == noisepage::type::TypeId::VARBINARY) {
        int string_compare_result =
            noisepage::storage::index::GenericKey::TypeComparators::CompareVarlens(lhs_attr, rhs_attr);
        if (string_compare_result < 0) return true;
        if (string_compare_result > 0) return false;
      } else {
        int key_compare_result = std::memcmp(lhs_attr, rhs_attr, attribute_size);
        if (key_compare_result < 0) return true;
        if (key_compare_result > 0) return false;
      }

      // attributes are equal, continue
    }

    // keys are equal
    return true;
  }

 private:
  ProjectedRow *GetProjectedRow() {
    auto *pr = reinterpret_cast<ProjectedRow *>(StorageUtil::AlignedPtr(sizeof(uint64_t), key_data_));
    NOISEPAGE_ASSERT(reinterpret_cast<uintptr_t>(pr) % sizeof(uint64_t) == 0,
                     "ProjectedRow must be aligned to 8 bytes for atomicity guarantees.");
    NOISEPAGE_ASSERT(reinterpret_cast<uintptr_t>(pr) + pr->Size() < reinterpret_cast<uintptr_t>(this) + data_size_,
                     "ProjectedRow will access out of bounds.");
    return pr;
  }

  byte *key_data_ = nullptr;
  uint64_t data_size_ = 0;
  const IndexMetadata *metadata_ = nullptr;
};

}  // namespace noisepage::storage::index

namespace std {

/**
 * Implements std::hash for GenericKey. Allows the class to be used with STL containers and the BwTree index.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template<>
struct hash<noisepage::storage::index::GenericKey> {
 public:
  /**
   * @param key key to be hashed
   * @return hash of the key's underlying data
   */
  size_t operator()(noisepage::storage::index::GenericKey const &key) const {
    const auto &metadata = key.GetIndexMetadata();

    const auto &key_schema = metadata.GetSchema();
    const auto &inlined_attr_sizes = metadata.GetInlinedAttributeSizes();

    uint64_t running_hash = 0;

    const auto *const pr = key.GetProjectedRow();

    const auto &key_cols = key_schema.GetColumns();
    for (uint16_t i = 0; i < key_cols.size(); i++) {
      const auto offset = pr->ColumnIds()[i].UnderlyingValue();
      const byte *const attr = pr->AccessWithNullCheck(offset);
      if (attr == nullptr) {
        continue;
      }

      running_hash = XXH3_64bits_withSeed(reinterpret_cast<const void *>(attr), inlined_attr_sizes[i], running_hash);
    }

    return running_hash;
  }
};

/**
 * Implements std::equal_to for GenericKey. Allows the class to be used with containers that expect STL interface.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template<>
struct equal_to<noisepage::storage::index::GenericKey> {
  /**
   * @param lhs first key to be compared
   * @param rhs second key to be compared
   * @return true if first key is equal to the second key
   */
  bool operator()(const noisepage::storage::index::GenericKey &lhs,
                  const noisepage::storage::index::GenericKey &rhs) const {
    const auto &key_schema = lhs.GetIndexMetadata().GetSchema();

    const auto &key_cols = key_schema.GetColumns();
    for (uint16_t i = 0; i < key_cols.size(); i++) {
      const auto *const lhs_pr = lhs.GetProjectedRow();
      const auto *const rhs_pr = rhs.GetProjectedRow();

      const auto offset = lhs_pr->ColumnIds()[i].UnderlyingValue();
      NOISEPAGE_ASSERT(lhs_pr->ColumnIds()[i] == rhs_pr->ColumnIds()[i], "Comparison orders should be the same.");

      const byte *const lhs_attr = lhs_pr->AccessWithNullCheck(offset);
      const byte *const rhs_attr = rhs_pr->AccessWithNullCheck(offset);

      if (lhs_attr == nullptr) {
        if (rhs_attr == nullptr) {
          // attributes are both NULL (equal), continue
          continue;
        }
        // lhs is NULL, rhs is non-NULL, return non-equal
        return false;
      }

      if (rhs_attr == nullptr) {
        // lhs is non-NULL, rhs is NULL, return non-equal
        return false;
      }

      const noisepage::type::TypeId type_id = key_schema.GetColumns()[i].Type();
      const uint16_t attribute_size = key_schema.GetColumns()[i].AttributeLength();
      if (type_id == noisepage::type::TypeId::VARCHAR || type_id == noisepage::type::TypeId::VARBINARY) {
        if (noisepage::storage::index::GenericKey::TypeComparators::CompareVarlens(lhs_attr, rhs_attr) != 0) {
          return false;
        }
      } else {
        if (std::memcmp(lhs_attr, rhs_attr, attribute_size) != 0) {
          return false;
        }
      }

      // attributes are equal, continue
    }

    // keys are equal
    return true;
  }
};

/**
 * Implements std::less for GenericKey. Allows the class to be used with containers that expect STL interface.
 * @tparam KeySize number of bytes for the key's internal buffer
 */
template<>
struct less<noisepage::storage::index::GenericKey> {
  /**
   * @param lhs first key to be compared
   * @param rhs second key to be compared
   * @return true if first key is less than the second key
   */
  bool operator()(const noisepage::storage::index::GenericKey &lhs,
                  const noisepage::storage::index::GenericKey &rhs) const {
    const auto &key_schema = lhs.GetIndexMetadata().GetSchema();
    const auto &key_cols = key_schema.GetColumns();

    for (uint16_t i = 0; i < key_cols.size(); i++) {
      const auto *const lhs_pr = lhs.GetProjectedRow();
      const auto *const rhs_pr = rhs.GetProjectedRow();

      const auto offset = lhs_pr->ColumnIds()[i].UnderlyingValue();
      NOISEPAGE_ASSERT(lhs_pr->ColumnIds()[i] == rhs_pr->ColumnIds()[i], "Comparison orders should be the same.");

      const byte *const lhs_attr = lhs_pr->AccessWithNullCheck(offset);
      const byte *const rhs_attr = rhs_pr->AccessWithNullCheck(offset);

      if (lhs_attr == nullptr) {
        if (rhs_attr == nullptr) {
          // attributes are both NULL (equal), continue
          continue;
        }
        // lhs is NULL, rhs is non-NULL, lhs is less than
        return true;
      }

      if (rhs_attr == nullptr) {
        // lhs is non-NULL, rhs is NULL, lhs is greater than
        return false;
      }

      const noisepage::type::TypeId type_id = key_schema.GetColumns()[i].Type();
      const uint16_t attribute_size = key_schema.GetColumns()[i].AttributeLength();
      if (type_id == noisepage::type::TypeId::VARCHAR || type_id == noisepage::type::TypeId::VARBINARY) {
        int string_compare_result =
            noisepage::storage::index::GenericKey::TypeComparators::CompareVarlens(lhs_attr, rhs_attr);
        if (string_compare_result < 0) return true;
        if (string_compare_result > 0) return false;
      } else {
        int key_compare_result = std::memcmp(lhs_attr, rhs_attr, attribute_size);
        if (key_compare_result < 0) return true;
        if (key_compare_result > 0) return false;
      }

      // attributes are equal, continue
    }

    // keys are equal
    return false;
  }
};
}  // namespace std