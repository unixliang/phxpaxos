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

#include "ioloop.h"
#include "utils_include.h"
#include "instance.h"

using namespace std;

namespace phxpaxos
{

IOLoop :: IOLoop(Config * poConfig, Group * poGroup)
    : m_poConfig(poConfig), m_poGroup(poGroup)
{
    m_bIsEnd = false;
    m_bIsStart = false;

    m_iQueueMemSize = 0;
}

IOLoop :: ~IOLoop()
{
}

void IOLoop :: run()
{
    m_bIsEnd = false;
    m_bIsStart = true;
    while(true)
    {
        BP->GetIOLoopBP()->OneLoop();

        int iNextTimeout = 1000;
        
        DealwithTimeout(iNextTimeout);

        //PLGHead("nexttimeout %d", iNextTimeout);

        OneLoop(iNextTimeout);

        if (m_bIsEnd)
        {
            PLGHead("IOLoop [End]");
            break;
        }
    }
}

void IOLoop :: AddNotify(std::shared_ptr<CommitCtx> & poCommitCtx)
{
    m_oCommitCtxQueue.lock();
    m_oCommitCtxQueue.add(poCommitCtx);
    m_oCommitCtxQueue.unlock();

    m_oMessageQueue.lock();
    m_oMessageQueue.add(nullptr);
    m_oMessageQueue.unlock();
}

int IOLoop :: AddMessage(const char * pcMessage, const int iMessageLen)
{
    m_oMessageQueue.lock();

    BP->GetIOLoopBP()->EnqueueMsg();

    if ((int)m_oMessageQueue.size() > QUEUE_MAXLENGTH)
    {
        BP->GetIOLoopBP()->EnqueueMsgRejectByFullQueue();

        PLGErr("Queue full, skip msg");
        m_oMessageQueue.unlock();
        return -2;
    }

    if (m_iQueueMemSize > MAX_QUEUE_MEM_SIZE)
    {
        PLErr("queue memsize %d too large, can't enqueue", m_iQueueMemSize);
        m_oMessageQueue.unlock();
        return -2;
    }
    
    m_oMessageQueue.add(new string(pcMessage, iMessageLen));

    m_iQueueMemSize += iMessageLen;

    m_oMessageQueue.unlock();

    return 0;
}

int IOLoop :: AddRetryPaxosMsg(const PaxosMsg & oPaxosMsg)
{
    BP->GetIOLoopBP()->EnqueueRetryMsg();

    if (m_oRetryQueue.size() > RETRY_QUEUE_MAX_LEN)
    {
        BP->GetIOLoopBP()->EnqueueRetryMsgRejectByFullQueue();
        m_oRetryQueue.pop();
    }
    
    m_oRetryQueue.push(oPaxosMsg);
    return 0;
}

void IOLoop :: Stop()
{
    m_bIsEnd = true;
    if (m_bIsStart)
    {
        join();
    }
}

void IOLoop :: ClearRetryQueue()
{
    while (!m_oRetryQueue.empty())
    {
        m_oRetryQueue.pop();
    }
}

void IOLoop :: DealWithRetry()
{
    if (m_oRetryQueue.empty())
    {
        return;
    }
    
    bool bHaveRetryOne = false;
    while (!m_oRetryQueue.empty())
    {
// TODO: 处理下一个窗口的消息
/*
        PaxosMsg & oPaxosMsg = m_oRetryQueue.front();
        if (oPaxosMsg.instanceid() > m_poInstance->GetNowInstanceID() + 1)
        {
            break;
        }
        else if (oPaxosMsg.instanceid() == m_poInstance->GetNowInstanceID() + 1)
        {
            //only after retry i == now_i, than we can retry i + 1.
            if (bHaveRetryOne)
            {
                BP->GetIOLoopBP()->DealWithRetryMsg();
                PLGDebug("retry msg (i+1). instanceid %lu", oPaxosMsg.instanceid());
                m_poInstance->OnReceivePaxosMsg(oPaxosMsg, true);
            }
            else
            {
                break;
            }
        }
        else if (oPaxosMsg.instanceid() == m_poInstance->GetNowInstanceID())
        {
            BP->GetIOLoopBP()->DealWithRetryMsg();
            PLGDebug("retry msg. instanceid %lu", oPaxosMsg.instanceid());
            m_poInstance->OnReceivePaxosMsg(oPaxosMsg);
            bHaveRetryOne = true;
        }
*/
        m_oRetryQueue.pop();
    }
}

void IOLoop :: CheckNewValue()
{
    if (!m_poGroup->GetLearner()->IsIMLatest())
    {
        return;
    }

    uint64_t llInstanceID{-1};
    bool use_idle_instance{false};
    if (!m_poGroup->HasTimeoutInstance(llInstanceID)) {
        use_idle_instance = m_poGroup->HasIdleInstance(llInstanceID);
    }
    if (NoCheckpoint == llInstanceID) {
        return;
    }

    int iCommitRet = PaxosTryCommitRet_OK;

    if (PaxosTryCommitRet_OK == iCommitRet && m_poConfig->IsIMFollower())
    {
        PLGErr("I'm follower, skip this new value");
        iCommitRet = PaxosTryCommitRet_Follower_Cannot_Commit;
    }
    if (PaxosTryCommitRet_OK == iCommitRet && !m_poConfig->CheckConfig())
    {
        PLGErr("I'm not in membership, skip this new value");
        iCommitRet = PaxosTryCommitRet_Im_Not_In_Membership;
    }

    if (0 == llInstanceID) { // proposal system variable first
        if (PaxosTryCommitRet_OK != iCommitRet) {
            return;
        }

        //Init system variables.
        PLGHead("Need to init system variables, InstanceID %lu Now.Gid %lu", 
                llInstanceID, m_poConfig->GetGid());

        uint64_t llGid = m_poConfig->GetIsUseMembership() ? OtherUtils::GenGid(m_poConfig->GetMyNodeID()) : 0;
        string sInitSVOpValue;
        int ret = m_poConfig->GetSystemVSM()->CreateGid_OPValue(llGid, sInitSVOpValue, m_poConfig->GetMaxWindowSize());
        assert(ret == 0);

        m_poGroup->GetSMFac()->PackPaxosValue(sInitSVOpValue, m_poConfig->GetSystemVSM()->SMID());
        m_poGroup->NewValue(llInstanceID, sInitSVOpValue, nullptr);

    } else {
        shared_ptr<CommitCtx> poCommitCtx;

        m_oCommitCtxQueue.lock();
        if (m_oCommitCtxQueue.empty())
        {
            m_oCommitCtxQueue.unlock();
            return;
        }

        m_oCommitCtxQueue.peek(poCommitCtx);
        m_oCommitCtxQueue.pop();

        m_oCommitCtxQueue.unlock();

        if (nullptr == poCommitCtx)
        {
            return;
        }

        if (PaxosTryCommitRet_OK != iCommitRet) {
            poCommitCtx->SetResultOnlyRet(iCommitRet);
            return;
        }

        if ((int)poCommitCtx->GetCommitValue().size() > MAX_VALUE_SIZE)
        {
            PLGErr("value size %zu to large, skip this new value",
                   poCommitCtx->GetCommitValue().size());
            poCommitCtx->SetResultOnlyRet(PaxosTryCommitRet_Value_Size_TooLarge);
            return;
        }

        poCommitCtx->StartCommit(llInstanceID);

        if (m_poGroup->GetOptions()->bOpenChangeValueBeforePropose) {
            m_poGroup->GetSMFac()->BeforePropose(m_poConfig->GetMyGroupIdx(), poCommitCtx->GetCommitValue());
        }
        m_poGroup->NewValue(llInstanceID, poCommitCtx->GetCommitValue(), poCommitCtx);
    }


    if (use_idle_instance) {
        m_poGroup->NewIdleInstance();
    }
}

void IOLoop :: OneLoop(const int iTimeoutMs)
{
    string *psMessage = nullptr;

    m_oMessageQueue.lock();
    bool bSucc = m_oMessageQueue.peek(psMessage, iTimeoutMs);
    
    if (!bSucc)
    {
        m_oMessageQueue.unlock();
    }
    else
    {
        m_oMessageQueue.pop();
        m_oMessageQueue.unlock();

        if (nullptr != psMessage && psMessage->size() > 0)
        {
            m_iQueueMemSize -= psMessage->size();

            m_poGroup->OnReceive(*psMessage);
        }

        delete psMessage;

        BP->GetIOLoopBP()->OutQueueMsg();
    }

    DealWithRetry();

    //must put on here
    //because addtimer on this funciton
    CheckNewValue();
}

bool IOLoop :: AddTimer(const int iTimeout, Timer::CallbackFunc fCallbackFunc, uint32_t & iTimerID)
{
    if (iTimeout == -1)
    {
        return true;
    }
    
    uint64_t llAbsTime = Time::GetSteadyClockMS() + iTimeout;
    m_oTimer.AddTimerWithCallbackFunc(llAbsTime, fCallbackFunc, iTimerID);

    m_mapTimerIDExist[iTimerID] = true;

    return true;
}

void IOLoop :: RemoveTimer(uint32_t & iTimerID)
{
    auto it = m_mapTimerIDExist.find(iTimerID);
    if (it != end(m_mapTimerIDExist))
    {
        m_mapTimerIDExist.erase(it);
    }

    iTimerID = 0;
}

void IOLoop :: DealwithTimeoutOne(const uint32_t iTimerID, Timer::CallbackFunc fCallbackFunc)
{
    auto it = m_mapTimerIDExist.find(iTimerID);
    if (it == end(m_mapTimerIDExist))
    {
        //PLGErr("Timeout aready remove!, timerid %u iType %d", iTimerID, iType);
        return;
    }

    m_mapTimerIDExist.erase(it);

    fCallbackFunc(iTimerID);
}

void IOLoop :: DealwithTimeout(int & iNextTimeout)
{
    bool bHasTimeout = true;

    while(bHasTimeout)
    {
        uint32_t iTimerID = 0;
        Timer::CallbackFunc fCallbackFunc = nullptr;
        bHasTimeout = m_oTimer.PopTimeout(iTimerID, fCallbackFunc);

        if (bHasTimeout)
        {
            DealwithTimeoutOne(iTimerID, fCallbackFunc);

            iNextTimeout = m_oTimer.GetNextTimeout();
            if (iNextTimeout != 0)
            {
                break;
            }
        }
    }
}

}


