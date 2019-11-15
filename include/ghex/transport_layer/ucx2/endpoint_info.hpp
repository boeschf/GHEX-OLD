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
#ifndef INCLUDED_GHEX_TL_UCX_ENDPOINT_INFO_HPP
#define INCLUDED_GHEX_TL_UCX_ENDPOINT_INFO_HPP

#include <iosfwd>
#include <vector>
#include "./endpoint.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct endpoint_info
                {
                    using uuid_type    = ::gridtools::ghex::tl::uuid::type; 
                    std::vector<unsigned char> m_data;

                    endpoint_info() = default;

                    endpoint_info(uuid_type id, const address& addr)
                    : m_data(sizeof(uuid_type)+addr.size())
                    {
                        std::memcpy(m_data.data(), &id, sizeof(uuid_type));
                        std::memcpy(m_data.data()+sizeof(uuid_type), addr.data(), addr.size());
                    }

                    endpoint_info(const endpoint_info&) = default;
                    endpoint_info(endpoint_info&&) = default;

                    endpoint_info& operator=(const endpoint_info&) = default;
                    endpoint_info& operator=(endpoint_info&&) = default;

                    void unpack(uuid_type& id, address& addr) const
                    {
                        addr = address(m_data.size()-sizeof(uuid_type));
                        std::memcpy(&id, m_data.data(), sizeof(uuid_type));
                        std::memcpy(addr.data(), m_data.data()+sizeof(uuid_type), addr.size());
                    }

                    std::size_t size() const noexcept { return m_data.size(); }
                    const unsigned char* data() const noexcept { return m_data.data(); }
                          unsigned char* data()       noexcept { return m_data.data(); }
                };
            
            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ENDPOINT_INFO_HPP */

