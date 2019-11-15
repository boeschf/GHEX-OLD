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
#ifndef INCLUDED_GHEX_TL_UCX_CONTEXT_HPP
#define INCLUDED_GHEX_TL_UCX_CONTEXT_HPP

#include "../context.hpp"
#include "./error.hpp"
#include "./communicator_base.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {

            namespace ucx {

                struct context
                {
                    using uuid_t = ::gridtools::ghex::tl::uuid::type;

                    struct type_erased_endpoint_db_t
                    {
                        struct iface
                        {
                            virtual int rank() = 0;
                            virtual int size() = 0;
                            virtual int est_size() = 0;
                            virtual void insert(uuid_t id, const address& addr) = 0;
                            virtual void erase(uuid_t id) = 0;
                            virtual void synchronize() = 0;
                            virtual address* find(uuid_t id) = 0;
                            virtual uuid_t* find(int rank, int index) = 0;
                            virtual ~iface() {}
                        };

                        template<typename Impl>
                        struct impl_t final : public iface
                        {
                            Impl m_impl;

                            impl_t(const Impl& impl) : m_impl{impl} {}
                            impl_t(Impl&& impl) : m_impl{std::move(impl)} {}

                            int rank() override { return m_impl.rank(); }
                            int size() override { return m_impl.size(); }
                            int est_size() override { return m_impl.est_size(); }
                            void insert(uuid_t id, const address& addr) override { m_impl.insert(id,addr); }
                            void erase(uuid_t id) override { m_impl.erase(id); }
                            void synchronize() override { m_impl.synchronize(); }
                            address* find(uuid_t id) override { return m_impl.find(id); }
                            uuid_t* find(int rank, int index) override { return m_impl.find(rank,index); }
                        };

                        std::unique_ptr<iface> m_impl;

                        template<typename Impl>
                        type_erased_endpoint_db_t(Impl&& impl)
                        : m_impl{
                            std::make_unique<
                                impl_t<
                                    typename std::remove_cv<
                                        typename std::remove_reference<
                                            Impl
                                        >::type
                                    >::type
                                >
                            >(std::forward<Impl>(impl))}
                        {}

                        inline int rank() const { return m_impl->rank(); }
                        inline int size() const { return m_impl->size(); }
                        inline int est_size() const { return m_impl->est_size(); }
                        inline void insert(uuid_t id, const address& addr) { m_impl->insert(id,addr); }
                        inline void erase(uuid_t id) { m_impl->erase(id); }
                        inline void synchronize() { m_impl->synchronize(); }
                        inline address* find(uuid_t id) { return m_impl->find(id); }
                        inline uuid_t* find(int rank, int index) { return m_impl->find(rank,index); }
                    };


                    type_erased_endpoint_db_t m_db;
                    ucp_context_h m_context;
                    std::size_t m_req_size;
                    
                    template<typename DB>
                    context(DB&& db)
                    : m_db{std::forward<DB>(db)}
                    {
                        // read run-time context
                        ucp_config_t* config_ptr;
                        GHEX_CHECK_UCX_RESULT(
                            ucp_config_read(NULL,NULL, &config_ptr)
                        );

                        // set parameters
                        ucp_params_t context_params;
                        // define valid fields
                        context_params.field_mask =
                            UCP_PARAM_FIELD_FEATURES          | // features
                            UCP_PARAM_FIELD_REQUEST_SIZE      | // size of reserved space in a non-blocking request
                            UCP_PARAM_FIELD_TAG_SENDER_MASK   | // mask which gets sender endpoint from a tag
                            UCP_PARAM_FIELD_MT_WORKERS_SHARED | // multi-threaded context: thread safety
                            UCP_PARAM_FIELD_ESTIMATED_NUM_EPS ; // estimated number of endpoints for this context

                        // features
                        context_params.features =
                            UCP_FEATURE_TAG                   ; // tag matching
                        // request size
                        //context_params.request_size = 64;
                        context_params.request_size = 0;
                        // thread safety
                        context_params.mt_workers_shared = 1;
                        // estimated number of connections
                        context_params.estimated_num_eps = m_db.est_size();
                        // mask
                        //context_params.tag_sender_mask  = 0x00000000fffffffful;
                        context_params.tag_sender_mask  = 0xfffffffffffffffful;

                        // initialize UCP
                        GHEX_CHECK_UCX_RESULT(
                            ucp_init(&context_params, config_ptr, &m_context)
                        );
                        ucp_config_release(config_ptr);

                        // check the actual parameters
                        ucp_context_attr_t attr;
                        attr.field_mask = 
                            UCP_ATTR_FIELD_REQUEST_SIZE | // internal request size
                            UCP_ATTR_FIELD_THREAD_MODE;   // thread safety
                        ucp_context_query(m_context, &attr);
                        m_req_size = attr.request_size;
                        if (attr.thread_mode != UCS_THREAD_MODE_MULTI)
                            throw std::runtime_error("ucx cannot be used with multi-threaded context");
                    }
                    
                    context(const context&) = delete;
                    context(context&&) noexcept = default;

                    ~context()
                    {
                        ucp_cleanup(m_context);
                    }

                    operator       ucp_context_h&()       noexcept { return m_context; }
                    operator const ucp_context_h&() const noexcept { return m_context; }

                    communicator_base make_communicator()
                    {
                        auto comm = communicator_base(this);
                        comm.m_context = this;
                        m_db.insert(comm.m_ep.m_id, comm.m_address);
                        return comm;
                    }

                    void notify_destruction(communicator_base* comm)
                    {
                        m_db.erase(comm->m_ep.m_id);
                    }

                    void synchronize()
                    {
                        m_db.synchronize();
                    }
                
                    uuid_t* get_id(int rank, int index)
                    {
                        return m_db.find(rank, index);
                    }
                
                    int rank() { return m_db.rank(); }
                    int size() { return m_db.size(); }
                };
                    
                

                
                communicator_base::~communicator_base()
                {
                    if (m_context)
                    {
                        for (const auto& kvp : m_ep_cache)
                        {
                            ucp_ep_close_nb(kvp.second.m_ep_h, UCP_EP_CLOSE_MODE_FLUSH);
                        }
                        m_context->notify_destruction(this);
                        ucp_ep_close_nb(m_ep.m_ep_h, UCP_EP_CLOSE_MODE_FLUSH);
                        ucp_worker_destroy(m_worker);
                    }
                }

                communicator_base::communicator_base(communicator_base&& other)
                : m_context{other.m_context}
                , m_worker{other.m_worker}
                , m_address{std::move(other.m_address)}
                , m_ep{other.m_ep}
                , m_ep_cache{std::move(other.m_ep_cache)}
                {
                    other.m_context = nullptr;
                }
                
                int communicator_base::rank() const { return m_context->m_db.rank(); }
                int communicator_base::size() const { return m_context->m_db.size(); }

                endpoint communicator_base::connect(uuid_t id)
                {
                    if (id == m_ep.m_id) return m_ep;
                    auto it = m_ep_cache.find(id);
                    if (it != m_ep_cache.end())
                        return it->second;
                    auto addr = m_context->m_db.find(id);
                    if (addr) 
                    {
                        ucp_address_t* remote_worker_address = addr->get();
                        ucp_ep_params_t ep_params;
                        ucp_ep_h ep_h;
                        ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
                        ep_params.address    = remote_worker_address;	    
                        GHEX_CHECK_UCX_RESULT(
                            ucp_ep_create(m_worker, &ep_params, &ep_h)
                        );
                        m_ep_cache[id] = endpoint{id, ep_h};
                        return {id, ep_h};
                    }
                    else 
                        throw std::runtime_error("endpoint is not available");
                }
                
                endpoint communicator_base::connect(int rank, int index)
                {
                    auto id = m_context->m_db.find(rank,index);
                    if (id)
                        return connect(*id);
                    else
                        throw std::runtime_error("endpoint is not available");
                }
                
                auto communicator_base::get_id(int rank, int index)
                {
                    return m_context->get_id(rank,index);
                }
                
            } // namespace ucx
            
            template<>
            struct context<ucx_tag>
            {
                using impl_type = ucx::context;
                std::unique_ptr<impl_type> m_impl;
                
                template<typename DB>
                context(DB&& db)
                : m_impl{ std::make_unique<impl_type>( std::forward<DB>(db) ) }
                {}

                auto make_communicator() { return m_impl->make_communicator(); }
                void synchronize() { m_impl->synchronize(); }
                
                auto get_id(int rank, int index) { return m_impl->get_id(rank,index); }

                int rank() { return m_impl->rank(); }
                int size() { return m_impl->size(); }
            }; 

        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_CONTEXT_HPP */

