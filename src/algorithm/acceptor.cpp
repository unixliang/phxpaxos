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

#include "acceptor.h"
#include "instance.h"
#include "paxos_log.h"
#include "crc32.h"
#include "group.h"

namespace phxpaxos
{

AcceptorState :: AcceptorState(const Config * poConfig, const LogStorage * poLogStorage, SoftState *poSoftState) :
  m_oPaxosLog(poLogStorage), m_iSyncTimes(0), m_poSoftState(poSoftState)
{
    m_poConfig = (Config *)poConfig;
    Init();
}

AcceptorState :: ~AcceptorState()
{
}

void AcceptorState :: Init()
{
    m_oAcceptedBallot.reset();
    
    m_sAcceptedValue = "";
}

const BallotNumber & AcceptorState :: GetAcceptedBallot() const
{
    return m_oAcceptedBallot;
}

void AcceptorState :: SetAcceptedBallot(const BallotNumber & oAcceptedBallot)
{
    m_oAcceptedBallot = oAcceptedBallot;
}

const std::string & AcceptorState :: GetAcceptedValue()
{
    return m_sAcceptedValue;
}

void AcceptorState :: SetAcceptedValue(const std::string & sAcceptedValue)
{
    m_sAcceptedValue = sAcceptedValue;
}

int AcceptorState :: Persist(const uint64_t llInstanceID, const BallotNumber & oPromiseBallot)
{
    AcceptorStateData oState;
    oState.set_instanceid(llInstanceID);
    oState.set_promiseid(oPromiseBallot.m_llProposalID);
    oState.set_promisenodeid(oPromiseBallot.m_llNodeID);
    oState.set_acceptedid(m_oAcceptedBallot.m_llProposalID);
    oState.set_acceptednodeid(m_oAcceptedBallot.m_llNodeID);
    oState.set_acceptedvalue(m_sAcceptedValue);
    oState.set_checksum(0);

    WriteOptions oWriteOptions;
    oWriteOptions.bSync = m_poConfig->LogSync();
    if (oWriteOptions.bSync)
    {
        m_iSyncTimes++;
        if (m_iSyncTimes > m_poConfig->SyncInterval())
        {
            m_iSyncTimes = 0;
        }
        else
        {
            oWriteOptions.bSync = false;
        }
    }

    int ret = m_oPaxosLog.WriteState(oWriteOptions, m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
    if (ret != 0)
    {
        return ret;
    }

    m_poSoftState->UpdateOnPersist(llInstanceID, oState);

    PLGImp("GroupIdx %d InstanceID %lu PromiseID %lu PromiseNodeID %lu "
            "AccectpedID %lu AcceptedNodeID %lu ValueLen %zu", 
            m_poConfig->GetMyGroupIdx(), llInstanceID, oPromiseBallot.m_llProposalID, 
            oPromiseBallot.m_llNodeID, m_oAcceptedBallot.m_llProposalID, 
            m_oAcceptedBallot.m_llNodeID, m_sAcceptedValue.size());
    
    return 0;
}

/////////////////////////////////////////////////////////////////////////////////

Acceptor :: Acceptor(
        const Config * poConfig, 
        const MsgTransport * poMsgTransport, 
        const LogStorage * poLogStorage,
        Group * poGroup)
  : Base(poConfig, poMsgTransport, poGroup), m_oAcceptorState(poConfig, poLogStorage, poGroup->GetSoftState()), m_poGroup(poGroup)
{
}

Acceptor :: ~Acceptor()
{
}

void Acceptor :: Init(uint64_t llInstanceID)
{
    SetInstanceID(llInstanceID);
    m_oAcceptorState.Init();
}

AcceptorState * Acceptor :: GetAcceptorState()
{
    return &m_oAcceptorState;
}

int Acceptor :: OnPrepare(const PaxosMsg & oPaxosMsg)
{
    PLGHead("START Msg.InstanceID %lu Msg.from_nodeid %lu Msg.ProposalID %lu",
            oPaxosMsg.instanceid(), oPaxosMsg.nodeid(), oPaxosMsg.proposalid());

    BP->GetAcceptorBP()->OnPrepare();
    
    PaxosMsg oReplyPaxosMsg;
    oReplyPaxosMsg.set_instanceid(GetInstanceID());
    oReplyPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oReplyPaxosMsg.set_proposalid(oPaxosMsg.proposalid());
    oReplyPaxosMsg.set_msgtype(MsgType_PaxosPrepareReply);

    BallotNumber oBallot(oPaxosMsg.proposalid(), oPaxosMsg.nodeid());

    auto poSoftState = m_poGroup->GetSoftState();
    uint64_t llEndPromiseInstanceID;
    auto oPromiseBallot = poSoftState->GetPromiseBallot(GetInstanceID(), llEndPromiseInstanceID);

    oReplyPaxosMsg.set_endpromiseinstanceid(llEndPromiseInstanceID);

    if (oBallot >= oPromiseBallot)
    {
        PLGDebug("[Promise] State.PromiseID %lu State.PromiseNodeID %lu "
                "State.PreAcceptedID %lu State.PreAcceptedNodeID %lu",
                oPromiseBallot.m_llProposalID, 
                oPromiseBallot.m_llNodeID,
                m_oAcceptorState.GetAcceptedBallot().m_llProposalID,
                m_oAcceptorState.GetAcceptedBallot().m_llNodeID);

        oReplyPaxosMsg.set_preacceptid(m_oAcceptorState.GetAcceptedBallot().m_llProposalID);
        oReplyPaxosMsg.set_preacceptnodeid(m_oAcceptorState.GetAcceptedBallot().m_llNodeID);

        if (m_oAcceptorState.GetAcceptedBallot().m_llProposalID > 0)
        {
            oReplyPaxosMsg.set_value(m_oAcceptorState.GetAcceptedValue());
        }

        int ret = m_oAcceptorState.Persist(GetInstanceID(), oBallot);
        if (ret != 0)
        {
            BP->GetAcceptorBP()->OnPreparePersistFail();
            PLGErr("Persist fail, Now.InstanceID %lu ret %d",
                    GetInstanceID(), ret);
            
            return -1;
        }

        BP->GetAcceptorBP()->OnPreparePass();
    }
    else
    {
        BP->GetAcceptorBP()->OnPrepareReject();

        PLGDebug("[Reject] State.PromiseID %lu State.PromiseNodeID %lu", 
                oPromiseBallot.m_llProposalID, 
                oPromiseBallot.m_llNodeID);
        
        oReplyPaxosMsg.set_rejectbypromiseid(oPromiseBallot.m_llProposalID);
    }

    nodeid_t iReplyNodeID = oPaxosMsg.nodeid();

    PLGHead("END Now.InstanceID %lu ReplyNodeID %lu",
            GetInstanceID(), oPaxosMsg.nodeid());;

    SendMessage(iReplyNodeID, oReplyPaxosMsg);

    return 0;
}

void Acceptor :: OnAccept(const PaxosMsg & oPaxosMsg)
{
    PLGHead("START Msg.InstanceID %lu Msg.from_nodeid %lu Msg.ProposalID %lu Msg.ValueLen %zu",
            oPaxosMsg.instanceid(), oPaxosMsg.nodeid(), oPaxosMsg.proposalid(), oPaxosMsg.value().size());

    BP->GetAcceptorBP()->OnAccept();

    PaxosMsg oReplyPaxosMsg;
    oReplyPaxosMsg.set_instanceid(GetInstanceID());
    oReplyPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oReplyPaxosMsg.set_proposalid(oPaxosMsg.proposalid());
    oReplyPaxosMsg.set_msgtype(MsgType_PaxosAcceptReply);

    BallotNumber oBallot(oPaxosMsg.proposalid(), oPaxosMsg.nodeid());

    auto poSoftState = m_poGroup->GetSoftState();
    uint64_t llEndPromiseInstanceID;
    auto oPromiseBallot = poSoftState->GetPromiseBallot(GetInstanceID(), llEndPromiseInstanceID);

    oReplyPaxosMsg.set_endpromiseinstanceid(llEndPromiseInstanceID);

    if (oBallot >= oPromiseBallot)
    {
        PLGDebug("[Promise] State.PromiseID %lu State.PromiseNodeID %lu "
                "State.PreAcceptedID %lu State.PreAcceptedNodeID %lu",
                oPromiseBallot.m_llProposalID, 
                oPromiseBallot.m_llNodeID,
                m_oAcceptorState.GetAcceptedBallot().m_llProposalID,
                m_oAcceptorState.GetAcceptedBallot().m_llNodeID);

        m_oAcceptorState.SetAcceptedBallot(oBallot);
        m_oAcceptorState.SetAcceptedValue(oPaxosMsg.value());
        
        int ret = m_oAcceptorState.Persist(GetInstanceID(), oBallot);
        if (ret != 0)
        {
            BP->GetAcceptorBP()->OnAcceptPersistFail();

            PLGErr("Persist fail, Now.InstanceID %lu ret %d",
                    GetInstanceID(), ret);
            
            return;
        }

        BP->GetAcceptorBP()->OnAcceptPass();
    }
    else
    {
        BP->GetAcceptorBP()->OnAcceptReject();

        PLGDebug("[Reject] State.PromiseID %lu State.PromiseNodeID %lu", 
                oPromiseBallot.m_llProposalID, 
                oPromiseBallot.m_llNodeID);
        
        oReplyPaxosMsg.set_rejectbypromiseid(oPromiseBallot.m_llProposalID);
    }

    nodeid_t iReplyNodeID = oPaxosMsg.nodeid();

    PLGHead("END Now.InstanceID %lu ReplyNodeID %lu",
            GetInstanceID(), oPaxosMsg.nodeid());

    SendMessage(iReplyNodeID, oReplyPaxosMsg);
}

}


