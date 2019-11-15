/* 
 * GridTools
 * 
 * Copyright (c) 2014-2019, ETH Zurich
 * All rights reserved.
 * 
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 * 
 */
#ifndef INCLUDED_GHEX_TL_UUID_HPP
#define INCLUDED_GHEX_TL_UUID_HPP

#include <stdint.h>
#include <atomic>

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace uuid {

                using type = std::uint_fast64_t;

                constexpr unsigned int num_lower_bits = 32u;
                constexpr type lower_mask = 0xfffffffful;
                constexpr type upper_mask = ~lower_mask;

                std::atomic<type> max_rank_index(0ul);

                type generate(int rank)
                {
                    return (((type)rank) << num_lower_bits) | max_rank_index++;
                }

            } // namespace uuid
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UUID_HPP */

