//
// Created by root on 12/26/25.
//

#include "OmniBackend.h"
#include "udf/UdfLoader.h"
#include "config/OmniConfig.h"
#include "memory/AllocationListener.h"
#include "memory/OmniPoolAllocationListener.h"
#include "memory/OmniBroadcastAllocationListener.h"
#include "memory/omni_task_memory_pool.h"
#include "operator/join/broadcast_hash_table_cache_control.h"

#include <stdexcept>
#include <cstdio>
#include <mutex>

namespace gluten {

class OmniBroadcastMemoryManager : public std::enable_shared_from_this<OmniBroadcastMemoryManager> {
public:
    explicit OmniBroadcastMemoryManager(std::unique_ptr<AllocationListener> listener)
        : listener_(std::move(listener))
    {
        if (listener_ == nullptr) {
            throw std::invalid_argument("Broadcast cache requires a Spark reservation listener");
        }
        coalesced_ = std::make_unique<OmniBroadcastAllocationListener>(*listener_);
    }

    std::shared_ptr<omniruntime::op::BroadcastEntryMemory> createEntryMemory()
    {
        class EntryMemory final : public omniruntime::op::BroadcastEntryMemory {
        public:
            explicit EntryMemory(std::shared_ptr<OmniBroadcastMemoryManager> domain)
                : domain_(std::move(domain)),
                  poolListener_(*domain_->coalesced_),
                  pool_(std::make_unique<omniruntime::mem::OmniTaskMemoryPool>(&poolListener_)) {}

            omniruntime::mem::OmniTaskMemoryPool* pool() const override { return pool_.get(); }

        private:
            // Pool releases its reservations before its listener domain dies.
            std::shared_ptr<OmniBroadcastMemoryManager> domain_;
            OmniPoolAllocationListener poolListener_;
            std::unique_ptr<omniruntime::mem::OmniTaskMemoryPool> pool_;
        };
        return std::make_shared<EntryMemory>(shared_from_this());
    }

    int64_t outstandingBytes() const { return listener_->currentBytes(); }

private:
    std::unique_ptr<AllocationListener> listener_;
    std::unique_ptr<OmniBroadcastAllocationListener> coalesced_;
};

namespace {
std::mutex broadcastLifecycleMutex;
}

std::shared_ptr<OmniBackend> OmniBackend::instance_ = nullptr;

void OmniBackend::create(const std::unordered_map<std::string, std::string> &conf)
{
    std::lock_guard<std::mutex> guard(broadcastLifecycleMutex);
    std::atomic_store(&instance_, std::shared_ptr<OmniBackend>(new OmniBackend(conf)));
}

void OmniBackend::configureBroadcastCache(std::unique_ptr<AllocationListener> listener)
{
    std::lock_guard<std::mutex> guard(broadcastLifecycleMutex);
    auto backend = std::atomic_load(&instance_);
    if (backend == nullptr || std::atomic_load(&backend->broadcastMemoryManager_) != nullptr) {
        throw std::logic_error("Omni backend is absent or broadcast cache already initialized");
    }
    auto domain = std::make_shared<OmniBroadcastMemoryManager>(std::move(listener));
    std::weak_ptr<OmniBroadcastMemoryManager> weakDomain = domain;
    omniruntime::op::ConfigureBroadcastCacheMemoryFactory([weakDomain] {
        auto owner = weakDomain.lock();
        if (owner == nullptr) {
            throw std::runtime_error("Broadcast cache memory domain has shut down");
        }
        return owner->createEntryMemory();
    });
    std::atomic_store(&backend->broadcastMemoryManager_, std::move(domain));
}

void OmniBackend::shutdownBroadcastCache()
{
    std::lock_guard<std::mutex> guard(broadcastLifecycleMutex);
    omniruntime::op::RetireAllBroadcastCacheEntries();
    auto backend = std::atomic_load(&instance_);
    if (backend != nullptr) {
        auto domain = std::atomic_exchange(&backend->broadcastMemoryManager_,
            std::shared_ptr<OmniBroadcastMemoryManager>{});
        const auto outstanding = domain == nullptr ? 0 : domain->outstandingBytes();
        if (outstanding != 0) {
            std::fprintf(stderr,
                "[OmniBHJCache] shutdown draining with %lld Spark storage bytes still pinned\n",
                static_cast<long long>(outstanding));
        }
    }
}

int64_t OmniBackend::broadcastCacheOutstandingBytes()
{
    // Hold owning snapshots throughout the listener call, without the lifecycle
    // mutex. Teardown can detach the domain but cannot destroy it under a reader.
    auto backend = std::atomic_load(&instance_);
    if (backend == nullptr) return 0;
    auto domain = std::atomic_load(&backend->broadcastMemoryManager_);
    return domain == nullptr ? 0 : domain->outstandingBytes();
}

void OmniBackend::init(const std::unordered_map<std::string, std::string> &conf)
{
    backendConf_ = std::make_shared<omniruntime::config::ConfigBase>(
        std::unordered_map<std::string, std::string>(conf));
    initUdf();
}

void OmniBackend::initUdf() const
{
    auto got = backendConf_->Get<std::string>(omniruntime::kOmniUdfLibraryPaths, "");
    if (!got.empty()) {
        auto udfLoader = UdfLoader::getInstance();
        udfLoader->loadUdfLibraries(got);
        udfLoader->registerUdf();
    }
}
}
