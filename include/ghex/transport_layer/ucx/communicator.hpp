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
#ifndef INCLUDED_GHEX_TL_UCX_COMMUNICATOR_HPP
#define INCLUDED_GHEX_TL_UCX_COMMUNICATOR_HPP

#include <boost/optional.hpp>
#include "../communicator.hpp"
#include "../mpi/setup.hpp"
#include "./communicator_base.hpp"
#include "./future.hpp"
//#include "./communicator_traits.hpp"

namespace gridtools {
    
    namespace ghex {

        namespace tl {

            /** UCX communicator which exposes basic non-blocking transport functionality and 
              * returns futures to await said transports to complete. */
            template<>
            class communicator<ucx_tag>
            : public ucx::communicator_base
            {
            public:
                using transport_type = ucx_tag;
                using base_type      = ucx::communicator_base;
                using address_type   = typename base_type::address_type;
                using rank_type      = typename base_type::rank_type;
                using size_type      = typename base_type::size_type;
                using tag_type       = typename base_type::tag_type;
                
                using allocator_type = typename base_type::allocator_type;
                using request        = ucx::internal_request<allocator_type>;
                //using status         = ucx::status;
                template<typename T>
                using future         = ucx::future<T, allocator_type>;
                //using traits         = ucx::communicator_traits;

            public:

                //communicator(const traits& t = traits{}) : base_type{t.communicator()} {}
                //communicator(const base_type& c) : base_type{c} {}
                communicator(const mpi::setup_communicator& c) : base_type{c} {}
                
                communicator(const communicator&) = default;
                communicator(communicator&&) noexcept = default;

                communicator& operator=(const communicator&) = default;
                communicator& operator=(communicator&&) noexcept = default;

                /** @return address of this process */
                const address_type& address() const { return std::get<0>(this->m_impl->m_map[rank()]);  }

            public: // send

                /** @brief non-blocking send
                  * @tparam T data type
                  * @param dest destination rank
                  * @param tag message tag
                  * @param buffer pointer to source buffer
                  * @param n number of elements in buffer
                  * @return completion handle */
                template<typename T>
                [[nodiscard]] future<void> send(rank_type dest, tag_type tag, const T* buffer, int n) const
                {
                    auto ep = std::get<1>(m_impl->m_map[dest]);
                    request req(m_impl->m_context.m_req_size, m_impl->m_worker, m_impl->m_request_pool.get_allocator());
                    const auto stag = ((std::uint_fast64_t)tag << 32) | (std::uint_fast64_t)(m_impl->m_rank);
                    auto status = ucp_tag_send_nbr(ep, buffer, sizeof(T)*n, ucp_dt_make_contig(1), stag, req.ucx_request_ptr());
                    if (status == UCS_OK) req.m_request->m_ready = true;
                    return req;
                }

            public: // recv

                /** @brief non-blocking receive
                  * @tparam T data type
                  * @param source source rank
                  * @param tag message tag
                  * @param buffer pointer destination buffer
                  * @param n number of elements in buffer
                  * @return completion handle */
                template<typename T>
                [[nodiscard]] future<void> recv(rank_type source, tag_type tag, T* buffer, int n) const
                {
                    request req(m_impl->m_context.m_req_size, m_impl->m_worker, m_impl->m_request_pool.get_allocator());
                    const auto rtag = ((std::uint_fast64_t)tag << 32) | (std::uint_fast64_t)(source);
                    /*auto status = */ucp_tag_recv_nbr( m_impl->m_worker, buffer, sizeof(T)*n, ucp_dt_make_contig(1), rtag, 0xfffffffffffffffful, req.ucx_request_ptr());
                    return req;
                }

            };

        } // namespace tl

    } // namespace ghex

} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_COMMUNICATOR_HPP */

