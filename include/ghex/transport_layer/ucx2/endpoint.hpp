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
                    using address_type = address;

                    uuid_type    m_id;
                    address_type m_address;
                    ucp_ep_h     m_ep_h;

                    endpoint() = default;
                    endpoint(uuid_type id, address_type&& addr, ucp_ep_h ep)
                    : m_id{id}, m_address{std::move(addr)}, m_ep_h{ep} {}

                    endpoint(const endpoint&) = delete;
                    endpoint& operator=(const endpoint&) = delete;

                    endpoint(endpoint&&) = default;
                    endpoint& operator=(endpoint&&) = default;

                    ~endpoint()
                    {
                        //ucp_ep_close_nb(m_ep_h, UCP_EP_CLOSE_MODE_FLUSH);
                    }

                    template<class CharT, class Traits = std::char_traits<CharT>>
                    friend std::basic_ostream<CharT,Traits>& operator<<(std::basic_ostream<CharT,Traits>& os, const endpoint& ep)
                    {
                        os << "endpoint{" << ep.m_id << "}";
                        return os;
                    }
                };
            
            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ENDPOINT_HPP */

