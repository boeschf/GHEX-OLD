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
#ifndef INCLUDED_GHEX_TL_UCX_ADDRESS_HPP
#define INCLUDED_GHEX_TL_UCX_ADDRESS_HPP

#include <memory>
#include <cstring>

#include "./error.hpp"


namespace gridtools{
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct address
                {
                    struct address_array_deleter
                    {
                        void operator()(unsigned char* ptr) const
                        {
                            delete[] ptr;
                        }
                    };

                    std::unique_ptr<unsigned char[], address_array_deleter> m_address_array;
                    std::size_t m_length;

                    address()
                    : m_length{0u}
                    {}

                    address(const ucp_address_t* address_handle, std::size_t address_length)
                    : m_address_array{ new unsigned char[address_length] }
                    , m_length{ address_length }
                    {
                        std::memcpy( m_address_array.get(), address_handle,  address_length);
                    }

                    address(const address&) = delete;
                    address(address&&) noexcept = default;
                    address& operator=(const address&) = delete;
                    address& operator=(address&&) noexcept = default;

                    const ucp_address_t* get() const noexcept { return reinterpret_cast<const ucp_address_t*>(m_address_array.get()); }
                    ucp_address_t*       get()       noexcept { return reinterpret_cast<ucp_address_t*>(m_address_array.get()); }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ADDRESS_HPP */

