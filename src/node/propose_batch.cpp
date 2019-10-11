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

#include "propose_batch.h"
#include <pthread.h>
#include "utils_include.h"
#include "comm_include.h"
#include "sm_base.h"

#include <chrono>
#include <vector>
#include <algorithm>
#include <iostream>

using namespace std;

namespace phxpaxos
{

uint64_t GetThreadID()
{
    return (uint64_t)pthread_self();
}

PendingProposal :: PendingProposal()
    : poSMCtx(nullptr), pllInstanceID(nullptr), 
    piBatchIndex(nullptr), llAbsEnqueueTime(0)
{
}

////////////////////////////////////////////////////////////////////

ProposeBatch :: ProposeBatch(const int iGroupIdx, Node * poPaxosNode, NotifierPool * poNotifierPool)
    : m_iMyGroupIdx(iGroupIdx), m_poPaxosNode(poPaxosNode), m_poNotifierPool(poNotifierPool), 
      alarm(new Alarm(m_iBatchDelayTimeMs,
                      static_cast<std::function<void()>>([this](){
                          this->Run();
                      }))),
     adaptiveAlarm(new Alarm(m_iAdaptiveBatchDelayTimeMs,
                      static_cast<std::function<void()>>([this](){
                          this->Run();
                      }))) {
    T = std::move(std::thread([this]() {
      while (true) {
        bool ok = false;
        auto P = this->Q.pop(ok);
        if (ok)
          P();
        else
          return;
      }
    }));

    callbackT = std::move(std::thread([this]() {
      while (true) {
        bool ok = false;
        auto P = this->callbackQ.pop(ok);
        if (ok)
          P();
        else
          return;
      }
    }));
}

ProposeBatch :: ~ProposeBatch() {
    Q.shutdown();
    callbackQ.shutdown();
    T.join();
    callbackT.join();
}

void ProposeBatch :: Start() {}

void ProposeBatch :: Stop()
{
    alarm->disarm();
    adaptiveAlarm->disarm();
    std::lock_guard<std::mutex> oLock(m_oMutex);
    m_bIsEnd = true;
    //notify all waiting thread.
    while (!m_oQueue.empty())
    {
        PendingProposal oPendingProposal = m_oQueue.front();
        auto func = [=]() {
            oPendingProposal.callback(Paxos_SystemError);
        };
        callbackQ.push(func);
        m_oQueue.pop();
    }
}

void ProposeBatch :: SetBatchCount(const int iBatchCount)
{
    m_iBatchCount = iBatchCount;
}

void ProposeBatch :: SetBatchAdaptiveDelayTimeMs(const int iBatchAdaptiveDelayTimeMs)
{
    adaptiveAlarm->resetTimeout(iBatchAdaptiveDelayTimeMs);
}

void ProposeBatch :: SetBatchDelayTimeMs(const int iBatchDelayTimeMs)
{
    m_iBatchDelayTimeMs = iBatchDelayTimeMs;
    alarm->resetTimeout(m_iBatchDelayTimeMs);
}

void ProposeBatch :: AsyncPropose(const std::string & sValue, uint64_t & llInstanceID, uint32_t & iBatchIndex, SMCtx * poSMCtx, std::function<void(int)> callback)
{
    //std::cerr<<"A\n";
    auto func = [=](int status) {
        if (status == PaxosTryCommitRet_OK)
            BP->GetCommiterBP()->BatchProposeOK();
        else
            BP->GetCommiterBP()->BatchProposeFail();
        callback(status);
    };
    //std::cerr<<"B\n";

    if (m_bIsEnd)
    {
        auto failfunc = [=]() {
            callback(Paxos_SystemError);
        };
        callbackQ.push(failfunc);
        return;
    }

    BP->GetCommiterBP()->BatchPropose();
    //std::cerr<<"C\n";
    AddProposal(sValue, llInstanceID, iBatchIndex, poSMCtx, func);
    //std::cerr<<"D\n";
}

int ProposeBatch :: Propose(const std::string & sValue, uint64_t & llInstanceID, uint32_t & iBatchIndex, SMCtx * poSMCtx)
{
    if (m_bIsEnd)
    {
        return Paxos_SystemError; 
    }

    BP->GetCommiterBP()->BatchPropose();

    uint64_t llThreadID = GetThreadID();

    Notifier * poNotifier = nullptr;
    int ret = m_poNotifierPool->GetNotifier(llThreadID, poNotifier);
    if (ret != 0)
    {
        PLG1Err("GetNotifier fail, ret %d", ret);
        BP->GetCommiterBP()->BatchProposeFail();
        return Paxos_SystemError;
    }

    auto func=[=](int status) {
        if (status == PaxosTryCommitRet_OK)
            BP->GetCommiterBP()->BatchProposeOK();
        else
            BP->GetCommiterBP()->BatchProposeFail();
        poNotifier->SendNotify(status);
    };

    AddProposal(sValue, llInstanceID, iBatchIndex, poSMCtx, func);

    poNotifier->WaitNotify(ret);
    return ret;
}

const bool ProposeBatch :: NeedBatch()
{
    if ((int)m_oQueue.size() >= m_iBatchCount
            || m_iNowQueueValueSize >= m_iBatchMaxSize)
    {
        return true;
    }
    else if (m_oQueue.size() > 0)
    {
        PendingProposal & oPendingProposal = m_oQueue.front();
        uint64_t llNowTime = Time::GetSteadyClockMS();
        int iProposalPassTime = llNowTime > oPendingProposal.llAbsEnqueueTime ?
            llNowTime - oPendingProposal.llAbsEnqueueTime : 0;
        if (iProposalPassTime > m_iBatchDelayTimeMs)
        {
            return true;
        }
    }

    return false;
}

void ProposeBatch :: AddProposal(const std::string & sValue, uint64_t & llInstanceID, uint32_t & iBatchIndex,
        SMCtx * poSMCtx, std::function<void(int)> callback)
{
    std::unique_lock<std::mutex> oLock(m_oMutex);
    if(!alarm->isArmed())
        alarm->rearm();
    adaptiveAlarm->rearm();

    PendingProposal oPendingProposal;
    oPendingProposal.callback = callback;
    oPendingProposal.psValue = sValue;
    oPendingProposal.poSMCtx = poSMCtx;
    oPendingProposal.pllInstanceID = &llInstanceID;
    oPendingProposal.piBatchIndex = &iBatchIndex;
    oPendingProposal.llAbsEnqueueTime = Time::GetSteadyClockMS();

    m_oQueue.push(oPendingProposal);
    m_iNowQueueValueSize += (int)oPendingProposal.psValue.size();
    //std::cerr<<"AddProposal\n";
    if (NeedBatch())
    {
        PLG1Debug("direct batch, queue size %zu value size %d", m_oQueue.size(), m_iNowQueueValueSize);

        vector<PendingProposal> vecRequest;
        PluckProposal(vecRequest);

        oLock.unlock();

        auto func=[=]() {
            this->DoPropose(vecRequest);
        };
        Q.push(func);
    }
}

void ProposeBatch :: Run()
{
    vector<PendingProposal> vecRequest;
    {    
        std::lock_guard<std::mutex> oLock(m_oMutex);
        adaptiveAlarm->disarm();
        alarm->disarm();
        if (m_bIsEnd)
            return;
        
        PluckProposal(vecRequest);
    }
    DoPropose(vecRequest);
}

void ProposeBatch :: PluckProposal(std::vector<PendingProposal> & vecRequest)
{
    int iPluckCount = 0;
    int iPluckSize = 0;

    uint64_t llNowTime = Time::GetSteadyClockMS();
    vecRequest.reserve(m_oQueue.size());
    while (!m_oQueue.empty())
    {
        PendingProposal & oPendingProposal = m_oQueue.front();
        vecRequest.push_back(oPendingProposal);

        iPluckCount++;
        iPluckSize += oPendingProposal.psValue.size();
        m_iNowQueueValueSize -= oPendingProposal.psValue.size();

        {
            int iProposalWaitTime = llNowTime > oPendingProposal.llAbsEnqueueTime ?
                llNowTime - oPendingProposal.llAbsEnqueueTime : 0;
            BP->GetCommiterBP()->BatchProposeWaitTimeMs(iProposalWaitTime);
        }

        m_oQueue.pop();

        //if (iPluckCount >= m_iBatchCount
        //        || iPluckSize >= m_iBatchMaxSize)
        //{
        //    break;
        //}
    }

    if (vecRequest.size() > 0)
    {
        PLG1Debug("pluck %zu request", vecRequest.size());
    }
}

void ProposeBatch :: OnlyOnePropose(const PendingProposal oPendingProposal)
{
    int ret = m_poPaxosNode->Propose(m_iMyGroupIdx, oPendingProposal.psValue,
            *oPendingProposal.pllInstanceID, oPendingProposal.poSMCtx);
    auto func = [=]() {
        oPendingProposal.callback(ret);
    };
    callbackQ.push(func);
}

void ProposeBatch :: DoPropose(const std::vector<PendingProposal> & vecRequest)
{
    //std::cerr<<"DoPropose\n";
    // No request
    if (vecRequest.empty())
        return;

    BP->GetCommiterBP()->BatchProposeDoPropose((int)vecRequest.size());

    if (vecRequest.size() == 1)
        return OnlyOnePropose(vecRequest[0]);

    BatchPaxosValues oBatchValues;
    BatchSMCtx oBatchSMCtx;
    oBatchSMCtx.m_vecSMCtxList.reserve(vecRequest.size());
    for (auto & oPendingProposal : vecRequest)
    {
        PaxosValue * poValue = oBatchValues.add_values();
        poValue->set_smid(oPendingProposal.poSMCtx != nullptr ? oPendingProposal.poSMCtx->m_iSMID : 0);
        poValue->set_value(oPendingProposal.psValue);

        oBatchSMCtx.m_vecSMCtxList.push_back(oPendingProposal.poSMCtx);
    }

    SMCtx oCtx;
    oCtx.m_iSMID = BATCH_PROPOSE_SMID;
    oCtx.m_pCtx = (void *)&oBatchSMCtx;

    string sBuffer;
    uint64_t llInstanceID = 0;
    int ret = 0;
    bool bSucc = oBatchValues.SerializeToString(&sBuffer);
    if (bSucc)
    {
        ret = m_poPaxosNode->Propose(m_iMyGroupIdx, sBuffer, llInstanceID, &oCtx);
        if (ret != 0)
        {
            PLG1Err("real propose fail, ret %d", ret);
        }
    }
    else
    {
        PLG1Err("BatchValues SerializeToString fail");
        ret = Paxos_SystemError;
    }

    for (size_t i = 0; i < vecRequest.size(); i++)
    {
        const PendingProposal oPendingProposal = vecRequest[i];
        *oPendingProposal.piBatchIndex = (uint32_t)i;
        *oPendingProposal.pllInstanceID = llInstanceID;
        auto func = [=]() {
            //std::cerr<<"DoPropose-run cb\n";
            oPendingProposal.callback(ret);
        };
        //std::cerr<<"DoPropose-add cb\n";
        callbackQ.push(func);
    }
}

}

