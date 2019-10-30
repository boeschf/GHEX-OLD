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
#include <iosfwd>

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

                    address(std::size_t length)
                    : m_address_array{ new unsigned char[length] }
                    , m_length{length}
                    {}

                    address(const ucp_address_t* address_handle, std::size_t address_length)
                    : m_address_array{ new unsigned char[address_length] }
                    , m_length{ address_length }
                    {
                        std::memcpy( m_address_array.get(), address_handle,  address_length);
                    }

                    address(const address& other)
                    : m_address_array{ new unsigned char[other.m_length] }
                    , m_length{other.m_length}
                    {
                        std::memcpy( m_address_array.get(), other.m_address_array.get(),  m_length);
                    }

                    address& operator=(const address& other)
                    {
                        m_address_array.reset( new unsigned char[other.m_length] );
                        m_length = other.m_length;
                        std::memcpy( m_address_array.get(), other.m_address_array.get(),  m_length);
                        return *this;
                    }

                    address(address&&) noexcept = default;
                    address& operator=(address&&) noexcept = default;

                    const ucp_address_t* get() const noexcept { return reinterpret_cast<const ucp_address_t*>(m_address_array.get()); }
                    ucp_address_t*       get()       noexcept { return reinterpret_cast<ucp_address_t*>(m_address_array.get()); }


                    template<class CharT, class Traits = std::char_traits<CharT>>
                    friend std::basic_ostream<CharT,Traits>& operator<<(std::basic_ostream<CharT,Traits>& os, const address& addr)
                    {
                        os << "address{";
                        os <<  std::hex;
                        for (unsigned int i=0; i<addr.m_length; ++i)
                        {
                            os << (unsigned int)(addr.m_address_array.get()[i]);
                        }
                        os << std::dec << "}";
                        return os;
                    }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ADDRESS_HPP */

