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
#include "./worker.hpp"
#include "./request.hpp"

namespace gridtools {
    
    namespace ghex {

        namespace tl {

            /** Mpi communicator which exposes basic non-blocking transport functionality and 
              * returns futures to await said transports to complete. */
            template<>
            class communicator<ucx_tag>
            {
            public: // member types

                using rank_type = typename ucx::worker_t::rank_type;
                using tag_type  = int;
                using request   = ucx::request;

            private: // members

                ucx::worker_t* m_send_worker;
                ucx::worker_t* m_recv_worker;

            public: // ctors

                communicator(ucx::worker_t* send_worker, ucx::worker_t* recv_worker)
                : m_send_worker(send_worker)
                , m_recv_worker(recv_worker)
                {}

            public: // member functions

                rank_type rank() const noexcept { return m_send_worker->rank(); }
                rank_type size() const noexcept { return m_send_worker->size(); }

                /*void test_connection(rank_type rank)
                {
                    const auto& ep = m_send_worker->connect(rank);
                }*/
                    
                static void empty_send_callback(void *, ucs_status_t) {}
                static void empty_recv_callback(void *, ucs_status_t, ucp_tag_recv_info_t*) {}
                    
                template<typename Message>
                request send(Message& msg, rank_type dst, tag_type tag)
                {
                    const auto& ep = m_send_worker->connect(dst);
                    const auto stag = ((std::uint_fast64_t)tag << 32) | 
                                       (std::uint_fast64_t)(rank());
                    ucs_status_ptr_t ret = ucp_tag_send_nb(
                        ep.get(),                                        // destination
                        msg.data(),                                      // buffer
                        msg.size()*sizeof(typename Message::value_type), // buffer size
                        ucp_dt_make_contig(1),                           // data type
                        stag,                                            // tag
                        &communicator::empty_send_callback);             // callback function pointer: empty here
                    if (reinterpret_cast<std::uintptr_t>(ret) == UCS_OK)
                    {
                        // send operation is completed immediately and the call-back function is not invoked
                        return {};
                    } 
                    else if(!UCS_PTR_IS_ERR(ret))
                    {
                        return {(void*)ret, m_send_worker};
                    }
                    else
                    {
                        // an error occurred
                        throw std::runtime_error("ghex: ucx error - send operation failed");
                    }
                }


                template<typename Message>
                request recv(Message& msg, rank_type src, tag_type tag)
                {
                    const auto rtag = ((std::uint_fast64_t)tag << 32) | 
                                       (std::uint_fast64_t)(src);
                    ucs_status_ptr_t ret = ucp_tag_recv_nb(
                        m_recv_worker->get(),                            // worker
                        msg.data(),                                      // buffer
                        msg.size()*sizeof(typename Message::value_type), // buffer size
                        ucp_dt_make_contig(1),                           // data type
                        rtag,                                            // tag
                        ~std::uint_fast64_t(0ul),                        // tag mask
                        &communicator::empty_recv_callback);             // callback function pointer: empty here
                    if(!UCS_PTR_IS_ERR(ret))
                    {
                        return {(void*)ret, m_recv_worker};
                    }
                    else
                    {
                        // an error occurred
                        throw std::runtime_error("ghex: ucx error - recv operation failed");
                    }
                }

            };

        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif // INCLUDED_GHEX_TL_UCX_COMMUNICATOR_HPP

