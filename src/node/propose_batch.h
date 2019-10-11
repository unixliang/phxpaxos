/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#pragma once

#include "commdef.h"
#include "utils_include.h"
#include "phxpaxos/node.h"
#include <mutex>
#include <memory>
#include <queue>
#include <condition_variable>
#include <thread>
#include <functional>

namespace phxpaxos
{

class PendingProposal
{
public:
    PendingProposal();
    std::string psValue;
    SMCtx * poSMCtx;

    //return parameter
    uint64_t * pllInstanceID; 
    uint32_t * piBatchIndex;

    // callback
    // void callback(int status);
    std::function<void(int)> callback;
    // Notifier * poNotifier;

    uint64_t llAbsEnqueueTime;
};

///////////////////////////////////

class ProposeBatch
{
public:
    ProposeBatch(const int iGroupIdx, Node * poPaxosNode, NotifierPool * poNotifierPool);
    virtual ~ProposeBatch();

    void Start();

    void Run();

    void Stop();

    int Propose(const std::string & sValue, uint64_t & llInstanceID, uint32_t & iBatchIndex, SMCtx * poSMCtx);

    void AsyncPropose(const std::string & sValue, uint64_t & llInstanceID, uint32_t & iBatchIndex, SMCtx * poSMCtx, std::function<void(int)> callback);

    void SetBatchCount(const int iBatchCount);

    void SetBatchDelayTimeMs(const int iBatchDelayTimeMs);

    void SetBatchAdaptiveDelayTimeMs(const int iBatchAdaptiveDelayTimeMs);

    virtual void DoPropose(const std::vector<PendingProposal> & vecRequest);

private:
    void AddProposal(const std::string & sValue, uint64_t & llInstanceID, uint32_t & iBatchIndex, 
            SMCtx * poSMCtx, std::function<void(int)> callback);
    void PluckProposal(std::vector<PendingProposal> & vecRequest);
    void OnlyOnePropose(const PendingProposal oPendingProposal);
    const bool NeedBatch();
    const int m_iMyGroupIdx;
    Node * m_poPaxosNode;
    NotifierPool * m_poNotifierPool;

    std::mutex m_oMutex;
    std::queue<PendingProposal> m_oQueue;
    bool m_bIsEnd = false;
    int m_iNowQueueValueSize = 0;
    int m_iBatchCount = 1024*1024;
    int m_iBatchDelayTimeMs = 100;
    int m_iAdaptiveBatchDelayTimeMs = 3;
    int m_iBatchMaxSize = 4*1024*1024;

    std::unique_ptr<Alarm> alarm, adaptiveAlarm;
    std::thread T, callbackT;
    ConcurrentQueue<std::function<void()>> Q, callbackQ;
};

}
