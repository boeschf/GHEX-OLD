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
#ifndef INCLUDED_GHEX_TL_UCX_ENDPOINT_HPP
#define INCLUDED_GHEX_TL_UCX_ENDPOINT_HPP

#include <iosfwd>
#include "./error.hpp"
#include "./address.hpp"
#include "../uuid.hpp"


namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct endpoint
                {
                    using uuid_type    = ::gridtools::ghex::tl::uuid::type;
                    uuid_type m_id;
                    ucp_ep_h m_ep_h;
                };
            
            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ENDPOINT_HPP */

