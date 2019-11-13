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
#ifndef INCLUDED_GHEX_TL_CALLBACK_COMMUNICATOR_TS_HPP
#define INCLUDED_GHEX_TL_CALLBACK_COMMUNICATOR_TS_HPP

#include <boost/lockfree/queue.hpp>
#include "./callback_communicator.hpp"

namespace gridtools
{
    namespace ghex
    {
        namespace tl {

            /** callback_communicator is a class to dispatch send and receive operations to. Each operation can 
              * optionally be tied to a user defined callback function / function object. The payload of each 
              * send/receive operation must be a ghex::shared_message_buffer<Allocator>. 
              * This class will keep a (shallow) copy of each message, thus it is safe to release the message at 
              * the caller's site.
              *
              * The user defined callback must define void operator()(message_type,rank_type,tag_type), where
              * message_type is a shared_message_buffer that can be cheaply copied/moved from within the callback body 
              * if needed.
              *
              * The communication must be explicitely progressed using the member function progress.
              *
              * An instance of this class is 
              * - a move-only.
              * - not thread-safe
              *
              * If unprogressed operations remain at time of destruction, std::terminate will be called.
              *
              * @tparam Communicator underlying transport communicator
              * @tparam Allocator    allocator type used for allocating shared message buffers */
            template<class Communicator, class Allocator = std::allocator<unsigned char>>
            class callback_communicator_ts
            {
            public: // member types
                
                using communicator_type = Communicator;
                using future_type       = typename communicator_type::template future<void>;
                using tag_type          = typename communicator_type::tag_type;
                using rank_type         = typename communicator_type::rank_type;
                using allocator_type    = Allocator;
                using message_type      = shared_message_buffer<allocator_type>;


                struct request_state
                {
                    volatile bool m_ready = false;
                    bool is_ready() const noexcept
                    {
                        return m_ready;
                    }
                };

                struct request
                {
                    std::shared_ptr<request_state> m_request_state;
                    bool is_ready() const noexcept 
                    {
                        return m_request_state->is_ready(); 
                    }
                };

            private: // member types

                // necessary meta information for each send/receive operation
                struct element_type
                {
                    using message_arg_type = message_type;
                    std::function<void(message_arg_type, rank_type, tag_type)> m_cb;
                    rank_type    m_rank;
                    tag_type     m_tag;
                    future_type  m_future;
                    message_type m_msg;
                    //std::weak_ptr<request_state> m_request_state;
                    std::shared_ptr<request_state> m_request_state;
                };
                using send_element_type   = element_type;
                using recv_element_type   = element_type;
                using lock_free_alloc_t   = boost::lockfree::allocator<std::allocator<unsigned char>>;
                using send_container_type = boost::lockfree::queue<send_element_type*, lock_free_alloc_t, boost::lockfree::fixed_sized<false>>;
                using recv_container_type = boost::lockfree::queue<recv_element_type*, lock_free_alloc_t, boost::lockfree::fixed_sized<false>>;


            private: // members

                communicator_type   m_comm;
                allocator_type      m_alloc;
                send_container_type m_sends;
                recv_container_type m_recvs;

            public: // ctors

                /** @brief construct from a basic transport communicator
                  * @param comm  the underlying transport communicator
                  * @param alloc the allocator instance to be used for constructing messages */
                callback_communicator_ts(const communicator_type& comm, allocator_type alloc = allocator_type{}) 
                : m_comm(comm), m_alloc(alloc), m_sends(128), m_recvs(128) {}
                callback_communicator_ts(communicator_type&& comm, allocator_type alloc = allocator_type{}) 
                : m_comm(std::move(comm)), m_alloc(alloc), m_sends(128), m_recvs(128)  {}

                callback_communicator_ts(const callback_communicator_ts&) = delete;
                callback_communicator_ts(callback_communicator_ts&&) = default;

                /** terminates the program if the queues are not empty */
                ~callback_communicator_ts() 
                { 
                    // consume all
                }
                
            public: // queries

                auto rank() const noexcept { return m_comm.rank(); }
                auto size() const noexcept { return m_comm.size(); }

                ///** returns the number of unprocessed send handles in the queue. */
                //std::size_t pending_sends() const { return m_sends.size(); }
                ///** returns the number of unprocessed recv handles in the queue. */
                //std::size_t pending_recvs() const { return m_recvs.size(); }

            public: // get a message

                /** get a message with size n from the communicator */
                message_type make_message(std::size_t n = 0u) const
                {
                    return { m_alloc, n };
                }

            public: // send

                /** @brief Send a message to a destination with the given tag and register a callback which will be 
                  * invoked when the send operation is completed.
                  * @tparam CallBack User defined callback class which defines 
                  *                  void Callback::operator()(message_type,rank_type,tag_type)
                  * @param msg Message to be sent
                  * @param dst Destination of the message
                  * @param tag Tag associated with the message
                  * @param cb  Callback function object */
                template<typename CallBack>
                request send(message_type msg, rank_type dst, tag_type tag, CallBack&& cb)
                {
                    GHEX_CHECK_CALLBACK
                    request req{std::make_shared<request_state>()};
                    auto element_ptr = new send_element_type{std::forward<CallBack>(cb), dst, tag, future_type{}, std::move(msg), req.m_request_state};
                    element_ptr->m_future = std::move( m_comm.send(element_ptr->m_msg, dst, tag) );
                    while (!m_sends.push(element_ptr)) {}
                    return req;
                }

                /** @brief Send a message without registering a callback. */
                request send(message_type msg, rank_type dst, tag_type tag)
                {
                    return send(std::move(msg),dst,tag,[](message_type,rank_type,tag_type){});
                }

                /** @brief Send a message to multiple destinations with the same rank an register an associated callback. 
                  * @tparam Neighs Range over rank_type
                  * @tparam CallBack User defined callback class which defines 
                  *                  void Callback::operator()(rank_type,tag_type,message_type)
                  * @param msg Message to be sent
                  * @param neighs Range of destination ranks
                  * @param tag Tag associated with the message
                  * @param cb Callback function object */
                template <typename Neighs, typename CallBack>
                std::vector<request> send_multi(message_type msg, Neighs const &neighs, int tag, CallBack&& cb)
                {
                    GHEX_CHECK_CALLBACK
                    using cb_type = typename std::remove_cv<typename std::remove_reference<CallBack>::type>::type;
                    auto cb_ptr = std::make_shared<cb_type>( std::forward<CallBack>(cb) );
                    std::vector<request> reqs;
                    for (auto id : neighs)
                        reqs.push_back( send(msg, id, tag,
                                [cb_ptr](message_type m, rank_type r, tag_type t)
                                {
                                    // if (cb_ptr->use_count == 1)
                                    (*cb_ptr)(std::move(m),r,t); 
                                }) );
                }

                /** @brief Send a message to multiple destinations without registering a callback */
                template <typename Neighs>
                std::vector<request> send_multi(message_type msg, Neighs const &neighs, int tag)
                {
                    return send_multi(std::move(msg),neighs,tag,[](message_type, rank_type,tag_type){});
                }

            public: // receive

                /** @brief Receive a message from a source rank with the given tag and register a callback which will
                  * be invoked when the receive operation is completed.
                  * @tparam CallBack User defined callback class which defines 
                  *                  void Callback::operator()(message_type,rank_type,tag_type)
                  * @param msg Message where data will be received
                  * @param src Source of the message
                  * @param tag Tag associated with the message
                  * @param cb  Callback function object */
                template<typename CallBack>
                request recv(message_type msg, rank_type src, tag_type tag, CallBack&& cb)
                {
                    GHEX_CHECK_CALLBACK
                    request req{std::make_shared<request_state>()};
                    auto element_ptr = new recv_element_type{std::forward<CallBack>(cb), src, tag, future_type{}, std::move(msg), req.m_request_state};
                    element_ptr->m_future = std::move( m_comm.recv(element_ptr->m_msg, src, tag) );
                    while (!m_recvs.push(element_ptr)) {}
                    return req;
                }

                /** @brief Receive a message with length size (storage is allocated accordingly). */
                template<typename CallBack>
                request recv(std::size_t size, rank_type src, tag_type tag, CallBack&& cb)
                {
                    return recv(message_type{size,m_alloc}, src, tag, std::forward<CallBack>(cb));
                }

                /** @brief Receive a message without registering a callback. */
                request recv(message_type msg, rank_type src, tag_type tag)
                {
                    return recv(std::move(msg),src,tag,[](message_type,rank_type,tag_type){});
                }

            public: // progress

                /** @brief Progress the communication. This function checks whether any receive and send operation is 
                  * completed and calls the associated callback (if it exists).
                  * @return returns false if all registered operations have been completed.*/
                std::size_t progress()
                {
                    std::size_t num_completed = 0u;
                    num_completed += run(m_sends);
                    num_completed += run(m_recvs);
                    return num_completed;
                }

                ///** @brief Progress the communication. This function checks whether any receive and send operation is 
                //  * completed and calls the associated callback (if it exists). When all registered operations have 
                //  * been completed this function checks for further unexpected incoming messages which will be received 
                //  * in a newly allocated shared_message_buffer and returned to the user through invocation of the 
                //  * provided callback.
                //  * @tparam CallBack User defined callback class which defines 
                //  *                  void Callback::operator()(message_type,rank_type,tag_type)
                //  * @param unexpected_cb callback function object
                //  * @return returns false if all registered operations have been completed. */
                //template<typename CallBack>
                //bool progress(CallBack&& unexpected_cb)
                //{
                //    GHEX_CHECK_CALLBACK
                //    const auto not_completed = progress();
                //    if (!not_completed)
                //    {
                //        if (auto o = m_comm.template recv_any_source_any_tag<message_type>(m_alloc))
                //        {
                //            auto t = o->get();
                //            unexpected_cb(std::move(std::get<2>(t)),std::get<0>(t),std::get<1>(t));
                //        }
                //    }
                //    return not_completed;
                //}

            public: // attach/detach
                
                ///** @brief Deregister a send operation from this object which matches the given destination and tag.
                //  * If such operation is found the callback will be discared and the message will be returned to the
                //  * caller together with a future on which completion can be awaited.
                //  * @param dst Destination of the message
                //  * @param tag Tag associated with the message
                //  * @return Either a pair of future and message or none */
                //boost::optional<std::pair<future_type,message_type>> detach_send(rank_type dst, tag_type tag)
                //{
                //    return detach(dst,tag,m_sends);
                //}

                ///** @brief Deregister a receive operation from this object which matches the given destination and tag.
                //  * If such operation is found the callback will be discared and the message will be returned to the 
                //  * caller together with a future on which completion can be awaited.
                //  * @param src Source of the message
                //  * @param tag Tag associated with the message
                //  * @return Either a pair of future and message or none */
                //boost::optional<std::pair<future_type,message_type>> detach_recv(rank_type src, tag_type tag)
                //{
                //    return detach(src,tag,m_recvs);
                //}

                ///** @brief Register a send operation with this object with future, destination and tag and associate it
                //  * with a callback. This is the inverse operation of detach. Note, that attaching of a send operation
                //  * originating from the underlying basic communicator is supported.
                //  * @tparam CallBack User defined callback class which defines 
                //  *                  void Callback::operator()(message_type,rank_type,tag_type)
                //  * @param fut future object
                //  * @param msg message data
                //  * @param dst destination rank
                //  * @param tag associated tag
                //  * @param cb  Callback function object */
                //template<typename CallBack>
                //void attach_send(future_type&& fut, message_type msg, rank_type dst, tag_type tag, CallBack&& cb)
                //{
                //    GHEX_CHECK_CALLBACK
                //    auto ptr = new send_element_type{ std::forward<CallBack>(cb), dst, tag, std::move(fut), std::move(msg) };
                //    while(!m_sends.push(ptr)) {}
                //}

                ///** @brief Register a send without associated callback. */
                //void attach_send(future_type&& fut, message_type msg, rank_type dst, tag_type tag)
                //{
                //    auto ptr = new send_element_type{ [](message_type,rank_type,tag_type){}, dst, tag, std::move(fut), std::move(msg) };
                //    while(!m_sends.push(ptr)) {}
                //}

                ///** @brief Register a receive operation with this object with future, source and tag and associate it
                //  * with a callback. This is the inverse operation of detach. Note, that attaching of a recv operation
                //  * originating from the underlying basic communicator is supported.
                //  * @tparam CallBack User defined callback class which defines 
                //  *                  void Callback::operator()(message_type,rank_type,tag_type)
                //  * @param fut future object
                //  * @param msg message data
                //  * @param dst source rank
                //  * @param tag associated tag
                //  * @param cb  Callback function object */
                //template<typename CallBack>
                //void attach_recv(future_type&& fut, message_type msg, rank_type src, tag_type tag, CallBack&& cb)
                //{
                //    GHEX_CHECK_CALLBACK
                //    auto ptr = new recv_element_type{ std::forward<CallBack>(cb), src, tag, std::move(fut), std::move(msg) };
                //    while(!m_recvs.push(ptr)) {}
                //}

                ///** @brief Register a receive without associated callback. */
                //void attach_recv(future_type&& fut, message_type msg, rank_type src, tag_type tag)
                //{
                //    auto ptr = new recv_element_type{ [](message_type,rank_type,tag_type){}, src, tag, std::move(fut), std::move(msg) };
                //    while(!m_recvs.push(ptr)) {}
                //}

            public: // cancel
                
                ///** @brief Deregister all operations from this object and attempt to cancel the communication.
                //  * @return true if cancelling was successful. */
                //bool cancel()
                //{
                //    const auto s = cancel_sends();
                //    const auto r = cancel_recvs();
                //    return s && r;
                //}

                ///** @brief Deregister all send operations from this object and attempt to cancel the communication.
                //  * @return true if cancelling was successful. */
                //bool cancel_sends() { return cancel(m_sends); }

                ///** @brief Deregister all recv operations from this object and attempt to cancel the communication.
                //  * @return true if cancelling was successful. */
                //bool cancel_recvs() { return cancel(m_recvs); }

            private: // implementation

                template<typename Queue>
                std::size_t run(Queue& d)
                {
                    send_element_type* ptr = nullptr;
                    if (d.pop(ptr))
                    {
                        if (ptr->m_future.ready())
                        {
                            // call the callback
                            ptr->m_cb(std::move(ptr->m_msg), ptr->m_rank, ptr->m_tag);
                            //ptr->m_request_state.lock()->m_ready = true;
                            ptr->m_request_state->m_ready = true;
                            delete ptr;
                            return 1u;
                        }
                        else
                        {
                            while( !d.push(ptr) ) {}
                            return 0u;
                        }
                    }
                    else return 0u;
                }

                //template<typename Queue>
                //boost::optional<std::pair<future_type,message_type>> detach(rank_type rank, tag_type tag, Queue& d)
                //{
                //    std::vector<send_element_type*> d2;
                //    send_element_type* found_ptr = nullptr;
                //    d.consume_all(
                //        [rank, tag,&d2,&found_ptr](auto ptr) 
                //        {
                //            if (ptr->m_rank == rank && ptr->m_tag == tag)
                //            {
                //                found_ptr = ptr;
                //            }
                //            else
                //            {
                //                d2.push_back(ptr);
                //            }
                //        });
                //    for (auto ptr : d2)
                //        while(!d.push(ptr)){}

                //    if (found_ptr)
                //    {
                //        auto cb =  std::move(found_ptr->m_cb);
                //        auto fut = std::move(found_ptr->m_future);
                //        auto msg = std::move(found_ptr->m_msg);
                //        delete found_ptr;
                //        return std::pair<future_type,message_type>{std::move(fut), std::move(msg)}; 
                //    }
                //    return boost::none;
                //}

                //template<typename Queue>
                //bool cancel(Queue& d)
                //{
                //    bool result = true;
                //    d.consume_all(
                //        [&result](auto ptr)
                //        {
                //            auto& fut = ptr->m_future;
                //            if (!fut.ready())
                //                result = result && fut.cancel();
                //            //else
                //            //    fut.wait();
                //            delete ptr;
                //        }
                //    );
                //    return result;
                //}
            };

        } // namespace tl
    } // namespace ghex
}// namespace gridtools

#endif/*INCLUDED_GHEX_TL_CALLBACK_COMMUNICATOR_TS_HPP */

