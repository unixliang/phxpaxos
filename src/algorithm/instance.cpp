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

#include "instance.h"
#include "proposer.h"
#include "acceptor.h"
#include "learner.h"

using namespace std;

namespace phxpaxos
{

Instance :: Instance(
        const Config * poConfig, 
        const LogStorage * poLogStorage,
        const MsgTransport * poMsgTransport,
        const Options & oOptions,
        Group * poGroup)
    :
    m_oAcceptor(poConfig, poMsgTransport, poLogStorage, poGroup),
    m_oProposer(poConfig, poMsgTransport, poGroup),
    m_oPaxosLog(poLogStorage),
    m_oOptions(oOptions),
    m_poGroup(poGroup)
{
    m_poConfig = (Config *)poConfig;
    m_poMsgTransport = (MsgTransport *)poMsgTransport;
    m_iCommitTimerID = 0;
}

Instance :: ~Instance()
{
    PLGHead("Instance Deleted, GroupIdx %d.", m_poConfig->GetMyGroupIdx());
}

int Instance :: Init(uint64_t llInstanceID)
{
    PLGImp("NowInstanceID %lu", llInstanceID);

    //proposer
    m_oProposer.Init(llInstanceID);

    //acceptor
    m_oAcceptor.Init(llInstanceID);

    m_llInstanceID = llInstanceID;

    return 0;
}

Proposer * Instance :: GetProposer()
{
    return &m_oProposer;
}

Acceptor * Instance :: GetAcceptor()
{
    return &m_oAcceptor;
}

void Instance :: SetCommitCtx(shared_ptr<CommitCtx> poCommitCtx)
{
    m_poCommitCtx = poCommitCtx;
}


shared_ptr<CommitCtx> Instance :: GetCommitCtx() const
{
    return m_poCommitCtx;
}


////////////////////////////////////////////////


void Instance :: OnNewValueCommitTimeout()
{
    BP->GetInstanceBP()->OnNewValueCommitTimeout();

    m_oProposer.ExitPrepare();
    m_oProposer.ExitAccept();

    if (m_poCommitCtx) {
        m_poCommitCtx->SetResult(PaxosTryCommitRet_Timeout, m_llInstanceID, "");
    }

    // for retry later
    m_poGroup->AddTimeoutInstance(m_llInstanceID);
}

//////////////////////////////////////////////////////////////////////

int Instance :: OnReceivePaxosMsg(const PaxosMsg & oPaxosMsg, const bool bIsRetry)
{
    BP->GetInstanceBP()->OnReceivePaxosMsg();

    PLGImp("Now.InstanceID %lu Msg.InstanceID %lu MsgType %d Msg.from_nodeid %lu My.nodeid %lu Seen.LatestInstanceID %lu",
           m_llInstanceID, oPaxosMsg.instanceid(), oPaxosMsg.msgtype(),
           oPaxosMsg.nodeid(), m_poConfig->GetMyNodeID(), m_poGroup->GetLearner()->GetSeenLatestInstanceID());

    if (oPaxosMsg.msgtype() == MsgType_PaxosPrepareReply
            || oPaxosMsg.msgtype() == MsgType_PaxosAcceptReply
            || oPaxosMsg.msgtype() == MsgType_PaxosProposal_SendNewValue)
    {
        if (!m_poConfig->IsValidNodeID(oPaxosMsg.nodeid()))
        {
            BP->GetInstanceBP()->OnReceivePaxosMsgNodeIDNotValid();
            PLGErr("acceptor reply type msg, from nodeid not in my membership, skip this message");
            return 0;
        }
        
        return ReceiveMsgForProposer(oPaxosMsg);
    }
    else if (oPaxosMsg.msgtype() == MsgType_PaxosPrepare
            || oPaxosMsg.msgtype() == MsgType_PaxosAccept)
    {
        //if my gid is zero, then this is a unknown node.
        if (m_poConfig->GetGid() == 0)
        {
            m_poConfig->AddTmpNodeOnlyForLearn(oPaxosMsg.nodeid());
        }
        
        if ((!m_poConfig->IsValidNodeID(oPaxosMsg.nodeid())))
        {
            PLGErr("prepare/accept type msg, from nodeid not in my membership(or i'm null membership), "
                    "skip this message and add node to tempnode, my gid %lu",
                    m_poConfig->GetGid());

            m_poConfig->AddTmpNodeOnlyForLearn(oPaxosMsg.nodeid());

            return 0;
        }

        //ChecksumLogic(oPaxosMsg);
        return ReceiveMsgForAcceptor(oPaxosMsg, bIsRetry);
    }
    else
    {
        BP->GetInstanceBP()->OnReceivePaxosMsgTypeNotValid();
        PLGErr("Invaid msgtype %d", oPaxosMsg.msgtype());
    }

    return 0;
}

int Instance :: ReceiveMsgForProposer(const PaxosMsg & oPaxosMsg)
{
    if (m_poConfig->IsIMFollower())
    {
        PLGErr("I'm follower, skip this message");
        return 0;
    }

    ///////////////////////////////////////////////////////////////
    
    if (oPaxosMsg.instanceid() != m_llInstanceID)
    {
/*
        if (oPaxosMsg.instanceid() + 1 == m_oProposer.GetInstanceID())
        {
            //Exipred reply msg on last instance.
            //If the response of a node is always slower than the majority node, 
            //then the message of the node is always ignored even if it is a reject reply.
            //In this case, if we do not deal with these reject reply, the node that 
            //gave reject reply will always give reject reply. 
            //This causes the node to remain in catch-up state.
            //
            //To avoid this problem, we need to deal with the expired reply.
            if (oPaxosMsg.msgtype() == MsgType_PaxosPrepareReply)
            {
                m_oProposer.OnExpiredPrepareReply(oPaxosMsg);
            }
            else if (oPaxosMsg.msgtype() == MsgType_PaxosAcceptReply)
            {
                m_oProposer.OnExpiredAcceptReply(oPaxosMsg);
            }
        }
*/
        BP->GetInstanceBP()->OnReceivePaxosProposerMsgInotsame();
        //PLGErr("InstanceID not same, skip msg");
        return 0;
    }

    if (oPaxosMsg.msgtype() == MsgType_PaxosPrepareReply)
    {
        m_oProposer.OnPrepareReply(oPaxosMsg);
    }
    else if (oPaxosMsg.msgtype() == MsgType_PaxosAcceptReply)
    {
        m_oProposer.OnAcceptReply(oPaxosMsg);
    }

    return 0;
}

int Instance :: ReceiveMsgForAcceptor(const PaxosMsg & oPaxosMsg, const bool bIsRetry)
{
    if (m_poConfig->IsIMFollower())
    {
        PLGErr("I'm follower, skip this message");
        return 0;
    }
    
    //////////////////////////////////////////////////////////////
    
    if (oPaxosMsg.instanceid() != m_oAcceptor.GetInstanceID())
    {
        BP->GetInstanceBP()->OnReceivePaxosAcceptorMsgInotsame();
    }

    /*
    if (oPaxosMsg.instanceid() == m_oAcceptor.GetInstanceID() + 1)
    {
        //skip success message
        PaxosMsg oNewPaxosMsg = oPaxosMsg;
        oNewPaxosMsg.set_instanceid(m_oAcceptor.GetInstanceID());
        oNewPaxosMsg.set_msgtype(MsgType_PaxosLearner_ProposerSendSuccess);

        ReceiveMsgForLearner(oNewPaxosMsg);
    }
    */
            
    if (oPaxosMsg.instanceid() == m_llInstanceID)
    {
        if (oPaxosMsg.msgtype() == MsgType_PaxosPrepare)
        {
            return m_oAcceptor.OnPrepare(oPaxosMsg);
        }
        else if (oPaxosMsg.msgtype() == MsgType_PaxosAccept)
        {
            m_oAcceptor.OnAccept(oPaxosMsg);
        }
    }

// TODO: 下一个窗口的消息进重试队列
/*
    else if ((!bIsRetry) && (oPaxosMsg.instanceid() > m_oAcceptor.GetInstanceID()))
    {
        //retry msg can't retry again.
        if (oPaxosMsg.instanceid() >= m_oLearner.GetSeenLatestInstanceID())
        {
            if (oPaxosMsg.instanceid() < m_oAcceptor.GetInstanceID() + RETRY_QUEUE_MAX_LEN)
            {
                //need retry msg precondition
                //1. prepare or accept msg
                //2. msg.instanceid > nowinstanceid. 
                //    (if < nowinstanceid, this msg is expire)
                //3. msg.instanceid >= seen latestinstanceid. 
                //    (if < seen latestinstanceid, proposer don't need reply with this instanceid anymore.)
                //4. msg.instanceid close to nowinstanceid.
                m_oIOLoop.AddRetryPaxosMsg(oPaxosMsg);
                
                BP->GetInstanceBP()->OnReceivePaxosAcceptorMsgAddRetry();

                //PLGErr("InstanceID not same, get in to retry logic");
            }
            else
            {
                //retry msg not series, no use.
                m_oIOLoop.ClearRetryQueue();
            }
        }
    }
*/
    return 0;
}

int Instance :: NewValue(const std::string & sValue)
{
    PLGDebug("(unix) InstanceID %lu", m_llInstanceID);

    return m_oProposer.NewValue(sValue);
}


///////////////////////////////

void Instance :: OnTimeout(const uint32_t iTimerID, const int iType)
{
    if (iType == Timer_Proposer_Prepare_Timeout)
    {
        m_oProposer.OnPrepareTimeout();
    }
    else if (iType == Timer_Proposer_Accept_Timeout)
    {
        m_oProposer.OnAcceptTimeout();
    }
    else if (iType == Timer_Learner_Askforlearn_noop)
    {
        m_poGroup->GetLearner()->AskforLearn_Noop();
    }
    else if (iType == Timer_Instance_Commit_Timeout)
    {
        OnNewValueCommitTimeout();
    }
    else
    {
        PLGErr("unknown timer type %d, timerid %u", iType, iTimerID);
    }
}

////////////////////////////////

/*
bool Instance :: SMExecute(
        const uint64_t llInstanceID, 
        const std::string & sValue, 
        SMCtx * poSMCtx)
{
    return m_oSMFac.Execute(m_poConfig->GetMyGroupIdx(), llInstanceID, sValue, poSMCtx);
}
*/

////////////////////////////////
/*
void Instance :: ChecksumLogic(const PaxosMsg & oPaxosMsg)
{
    if (oPaxosMsg.lastchecksum() == 0)
    {
        return;
    }

    if (oPaxosMsg.instanceid() != m_oAcceptor.GetInstanceID())
    {
        return;
    }

    if (m_oAcceptor.GetInstanceID() > 0 && GetLastChecksum() == 0)
    {
        PLGErr("I have no last checksum, other last checksum %u", oPaxosMsg.lastchecksum());
        m_iLastChecksum = oPaxosMsg.lastchecksum();
        return;
    }
    
    PLGHead("my last checksum %u other last checksum %u", GetLastChecksum(), oPaxosMsg.lastchecksum());

    if (oPaxosMsg.lastchecksum() != GetLastChecksum())
    {
        PLGErr("checksum fail, my last checksum %u other last checksum %u", 
                 GetLastChecksum(), oPaxosMsg.lastchecksum());
        BP->GetInstanceBP()->ChecksumLogicFail();
    }

    assert(oPaxosMsg.lastchecksum() == GetLastChecksum());
}
*/
//////////////////////////////////////////



}


