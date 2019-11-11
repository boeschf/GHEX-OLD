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
#ifndef INCLUDED_GHEX_TL_MPI_ADDRESS_HPP
#define INCLUDED_GHEX_TL_MPI_ADDRESS_HPP

#include <cstring>
#include <iosfwd>
#include <vector>
#include "./error.hpp"

namespace gridtools{
    namespace ghex {
        namespace tl {
            namespace mpi {

                struct address
                {
                    std::vector<unsigned char> m_buffer;

                    address() = default;

                    address(std::size_t length) : m_buffer(length) {}

                    template<typename ForwardIterator>
                    address(ForwardIterator first, ForwardIterator last)
                    : m_buffer(first,last) {}

                    address(const address& other) = default;
                    address& operator=(const address& other) = default;
                    address(address&&) noexcept = default;
                    address& operator=(address&&) noexcept = default;

                    std::size_t size() const noexcept { return m_buffer.size(); }
                    
                    const unsigned char* data() const noexcept { return m_buffer.data(); }
                          unsigned char* data()       noexcept { return m_buffer.data(); }

                    const ucp_address_t* get() const noexcept { return reinterpret_cast<const ucp_address_t*>(m_buffer.data()); }
                    ucp_address_t*       get()       noexcept { return reinterpret_cast<ucp_address_t*>(m_buffer.data()); }

                    auto  begin() const noexcept { return m_buffer.begin(); }
                    auto  begin()       noexcept { return m_buffer.begin(); }
                    auto cbegin() const noexcept { return m_buffer.cbegin(); }

                    auto  end() const noexcept { return m_buffer.end(); }
                    auto  end()       noexcept { return m_buffer.end(); }
                    auto cend() const noexcept { return m_buffer.cend(); }

                    unsigned char  operator[](std::size_t i) const noexcept { return m_buffer[i]; }
                    unsigned char& operator[](std::size_t i)       noexcept { return m_buffer[i]; }

                    template<class CharT, class Traits = std::char_traits<CharT>>
                    friend std::basic_ostream<CharT,Traits>& operator<<(std::basic_ostream<CharT,Traits>& os, const address& addr)
                    {
                        os << "address{";
                        os <<  std::hex;
                        for (auto c : addr)
                            os << (unsigned int)c;
                        os << std::dec << "}";
                        return os;
                    }
                };

            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_ADDRESS_HPP */

