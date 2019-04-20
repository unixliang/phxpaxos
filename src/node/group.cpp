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

#include "group.h"

namespace phxpaxos
{


Group :: Group(LogStorage * poLogStorage, 
            NetWork * poNetWork,    
            InsideSM * poMasterSM,
            const int iGroupIdx,
            const Options & oOptions) : 
    m_poLogStorage(poLogStorage),
    m_oCommunicate(&m_oConfig, oOptions.oMyNode.GetNodeID(), oOptions.iUDPMaxSize, poNetWork),
    m_oConfig(poLogStorage, oOptions.bSync, oOptions.iSyncInterval, oOptions.bUseMembership, 
            oOptions.oMyNode, oOptions.vecNodeInfoList, oOptions.vecFollowerNodeInfoList, 
            iGroupIdx, oOptions.iGroupCount, oOptions.pMembershipChangeCallback),
    m_oInstance(&m_oConfig, poLogStorage, &m_oCommunicate, oOptions, this),
    m_oSMFac(m_oConfig.GetMyGroupIdx()),
    m_oCheckpointMgr(&m_oConfig, &m_oSMFac, poLogStorage, oOptions.bUseCheckpointReplayer),
    m_oLearner(&m_oConfig, &m_oCommunicate, this, poLogStorage, &m_oIOLoop, &m_oCheckpointMgr, &m_oSMFac),
    m_iInitRet(-1), m_poThread(nullptr)
{
    m_oConfig.SetMasterSM(poMasterSM);
}

Group :: ~Group()
{
}

void Group :: StartInit()
{
    m_poThread = new std::thread(&Group::Init, this);
    assert(m_poThread != nullptr);
}


int Group :: Load(uint64_t & llInstanceID)
{
    // maybe it has max ballot
    int ret = GetMaxInstanceIDFromLog(llInstanceID);
    if (ret != 0 && ret != 1)
    {
        PLGErr("Load max instance id fail, ret %d", ret);
        return ret;
    }

    if (ret == 1)
    {
        PLGErr("empty database");
        llInstanceID = 0;
        return 0;
    }

    AcceptorStateData oState;
    ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
    if (ret != 0)
    {
        return ret;
    }

    m_llProposalID = oState.promiseid();
    m_mapInstanceID2PromiseBallot.clear();
    m_mapInstanceID2PromiseBallot[llInstanceID] = BallotNumber(oState.promiseid(), oState.promisenodeid());

   /*
    m_oPromiseBallot.m_llProposalID = oState.promiseid();
    m_oPromiseBallot.m_llNodeID = oState.promisenodeid();
    m_oAcceptedBallot.m_llProposalID = oState.acceptedid();
    m_oAcceptedBallot.m_llNodeID = oState.acceptednodeid();
    m_sAcceptedValue = oState.acceptedvalue();
    m_iChecksum = oState.checksum();
    */

    PLGImp("GroupIdx %d InstanceID %lu PromiseID %lu PromiseNodeID %lu"
           " AccectpedID %lu AcceptedNodeID %lu ValueLen %zu Checksum %u", 
            m_poConfig->GetMyGroupIdx(), llInstanceID, m_oPromiseBallot.m_llProposalID, 
            m_oPromiseBallot.m_llNodeID, m_oAcceptedBallot.m_llProposalID, 
            m_oAcceptedBallot.m_llNodeID, m_sAcceptedValue.size(), m_iChecksum);
    
    return 0;
}

int Group :: InitLastCheckSum()
{
    if (m_llNowInstanceID == 0)
    {
        m_iLastChecksum = 0;
        return 0;
    }

    if (m_llNowInstanceID <= m_oCheckpointMgr.GetMinChosenInstanceID())
    {
        m_iLastChecksum = 0;
        return 0;
    }

    AcceptorStateData oState;
    int ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), m_llNowInstanceID - 1, oState);
    if (ret != 0 && ret != 1)
    {
        return ret;
    }

    if (ret == 1)
    {
        PLGErr("las checksum not exist, now instanceid %lu", m_llNowInstanceID);
        m_iLastChecksum = 0;
        return 0;
    }

    m_iLastChecksum = oState.checksum();

    PLGImp("ok, last checksum %u", m_iLastChecksum);

    return 0;
}

void Group :: Init()
{
    m_iInitRet = m_oConfig.Init();
    if (m_iInitRet != 0)
    {
        return;
    }

    m_iInitRet = m_oCheckpointMgr.Init();
    if (m_iInitRet != 0)
    {
        return;
    }

    //inside sm
    AddStateMachine(m_oConfig.GetSystemVSM());
    AddStateMachine(m_oConfig.GetMasterSM());
    
    //load cp instanceid
    uint64_t llCPInstanceID = m_oCheckpointMgr.GetCheckpointInstanceID() + 1;

    //load max instanceid
    uint64_t llMaxInstanceID{0};
    int m_iInitRet = GetMaxInstanceIDFromLog(llMaxInstanceID);
    if (m_iInitRet != 0 && m_iInitRet != 1)
    {
        //PLGErr("Load max instance id fail, ret %d", ret);
        return;
    }

    if (m_iInitRet == 1)
    {
        //PLGErr("empty database");
        m_iInitRet = 0;
        llMaxInstanceID = 0;
    }

    //PLGImp("Acceptor.OK, Log.InstanceID %lu Checkpoint.InstanceID %lu", 
    //        m_oAcceptor.GetInstanceID(), llCPInstanceID);

    //playlog
    {
        m_llNowInstanceID = llCPInstanceID;
        if (m_llNowInstanceID < llMaxInstanceID)
        {
            m_iInitRet = PlayLog(m_llNowInstanceID, llMaxInstanceID);
            if (m_iInitRet != 0)
            {
                return;
            }

            //PLGImp("PlayLog OK, begin instanceid %lu end instanceid %lu", m_llNowInstanceID, m_oAcceptor.GetInstanceID());

            m_llNowInstanceID = llMaxInstanceID;
        }
        else
        {
            if (m_llNowInstanceID > llMaxInstanceID)
            {
                m_iInitRet = ProtectionLogic_IsCheckpointInstanceIDCorrect(m_llNowInstanceID, llMaxInstanceID);
                if (m_iInitRet != 0)
                {
                    return m_iInitRet;
                }
                //m_oAcceptor.InitForNewPaxosInstance();
            }

            //m_oAcceptor.SetInstanceID(m_llNowInstanceID);
        }

        PLGImp("NowInstanceID %lu", m_llNowInstanceID);

        m_oLearner.SetInstanceID(m_llNowInstanceID);
        //m_oProposer.SetInstanceID(m_llNowInstanceID);

        m_oCheckpointMgr.SetMaxChosenInstanceID(m_llNowInstanceID);

        m_iInitRet = InitLastCheckSum();
        if (m_iInitRet != 0)
        {
            return;
        }

        m_oLearner.Reset_AskforLearn_Noop();

        PLGImp("OK");
    }


}

int Group :: GetInitRet()
{
    m_poThread->join();
    delete m_poThread;

    return m_iInitRet;
}

void Group :: Start()
{
    m_oInstance.Start();
}

void Group :: Stop()
{
    m_oInstance.Stop();
}

Config * Group :: GetConfig()
{
    return &m_oConfig;
}

Instance * Group :: GetInstanceByInstanceID(uint64_t llInstanceID)
{
    // TODO
}

Instance * GetCurrentInstance() {
    // TODO
}

Committer * Group :: GetCommitter()
{
    return m_oInstance.GetCommitter();
}

Cleaner * Group :: GetCheckpointCleaner()
{
    return m_oInstance.GetCheckpointCleaner();
}

Replayer * Group :: GetCheckpointReplayer()
{
    return m_oInstance.GetCheckpointReplayer();
}

void Group :: AddStateMachine(StateMachine * poSM)
{
    m_oInstance.AddStateMachine(poSM);
}

uint64_t Group :: GetProposalID() const
{
    return m_llProposalID;
}

void Group :: NewPrepare()
{
    PLGHead("START ProposalID %lu HighestOther %lu MyNodeID %lu",
            m_llProposalID, m_llHighestOtherProposalID, m_poConfig->GetMyNodeID());

    uint64_t llMaxProposalID =
        m_llProposalID > m_llHighestOtherProposalID ? m_llProposalID : m_llHighestOtherProposalID;

    m_llProposalID = llMaxProposalID + 1;

    PLGHead("END New.ProposalID %lu", m_llProposalID);
}


void Group :: SetOtherProposalID(const uint64_t llOtherProposalID)
{
    if (llOtherProposalID > m_llHighestOtherProposalID)
    {
        m_llHighestOtherProposalID = llOtherProposalID;
    }
}

uint32_t Group :: GetMaxWindowSize()
{
    return m_iMaxWindowSize;
}


void Group :: SetPromiseBallotForAcceptor(const uint64_t llInstanceID, const BallotNumber &oBallotNumber)
{
    uint64_t llEndPromiseInstanceID{-1};
    BallotNumber oPromiseBallotNumber = GetPromiseBallotForAcceptor(llInstanceID, llEndPromiseInstanceID);

    if (oBallotNumber <= oPromiseBallotNumber) return;

    m_mapInstanceID2PromiseBallot[llInstanceID] = oBallotNumber;
    while (m_mapInstanceID2PromiseBallot.size() > GetMaxWindowSize())
    {
        m_mapInstanceID2PromiseBallot.erase(m_mapInstanceID2PromiseBallot.begin());
    }
}

BallotNumber Group :: GetPromiseBallotForAcceptor(const uint64_t llInstanceID, uint64_t & llEndPromiseInstanceID) const
{
    llEndPromiseInstanceID = -1;

    auto it = m_mapInstanceID2PromiseBallot.upper_bound(llInstanceID);
    if (m_mapInstanceID2PromiseBallot.end() != it) {
        llEndPromiseInstanceID = it->first;
    }
    if (m_mapInstanceID2PromiseBallot.begin() == it) {
        return BallotNumber();
    }

    return --it;
}

void Group :: OnReceiveCheckpointMsg(const CheckpointMsg & oCheckpointMsg)
{
    PLGImp("Now.InstanceID %lu MsgType %d Msg.from_nodeid %lu My.nodeid %lu flag %d"
            " uuid %lu sequence %lu checksum %lu offset %lu buffsize %zu filepath %s",
            m_oAcceptor.GetInstanceID(), oCheckpointMsg.msgtype(), oCheckpointMsg.nodeid(),
            m_oConfig.GetMyNodeID(), oCheckpointMsg.flag(), oCheckpointMsg.uuid(), oCheckpointMsg.sequence(), oCheckpointMsg.checksum(),
            oCheckpointMsg.offset(), oCheckpointMsg.buffer().size(), oCheckpointMsg.filepath().c_str());

    if (oCheckpointMsg.msgtype() == CheckpointMsgType_SendFile)
    {
        if (!m_oCheckpointMgr.InAskforcheckpointMode())
        {
            PLGImp("not in ask for checkpoint mode, ignord checkpoint msg");
            return;
        }

        m_oLearner.OnSendCheckpoint(oCheckpointMsg);
    }
    else if (oCheckpointMsg.msgtype() == CheckpointMsgType_SendFile_Ack)
    {
        m_oLearner.OnSendCheckpointAck(oCheckpointMsg);
    }
}

bool Group :: ReceiveMsgHeaderCheck(const Header & oHeader, const nodeid_t iFromNodeID)
{
    if (m_oConfig.GetGid() == 0 || oHeader.gid() == 0)
    {
        return true;
    }

    if (m_oConfig.GetGid() != oHeader.gid())
    {
        BP->GetAlgorithmBaseBP()->HeaderGidNotSame();
        PLGErr("Header check fail, header.gid %lu config.gid %lu, msg.from_nodeid %lu",
                oHeader.gid(), m_oConfig.GetGid(), iFromNodeID);
        return false;
    }

    return true;
}

void Group :: OnReceive(const std::string & sBuffer)
{
    BP->GetInstanceBP()->OnReceive();

    if (sBuffer.size() <= 6)
    {
        PLGErr("buffer size %zu too short", sBuffer.size());
        return;
    }

    Header oHeader;
    size_t iBodyStartPos = 0;
    size_t iBodyLen = 0;
    int ret = Base::UnPackBaseMsg(sBuffer, oHeader, iBodyStartPos, iBodyLen);
    if (ret != 0)
    {
        return;
    }

    int iCmd = oHeader.cmdid();

    if (iCmd == MsgCmd_PaxosMsg)
    {
        if (m_oCheckpointMgr.InAskforcheckpointMode())
        {
            PLGImp("in ask for checkpoint mode, ignord paxosmsg");
            return;
        }
        
        PaxosMsg oPaxosMsg;
        bool bSucc = oPaxosMsg.ParseFromArray(sBuffer.data() + iBodyStartPos, iBodyLen);
        if (!bSucc)
        {
            BP->GetInstanceBP()->OnReceiveParseError();
            PLGErr("PaxosMsg.ParseFromArray fail, skip this msg");
            return;
        }

        if (!ReceiveMsgHeaderCheck(oHeader, oPaxosMsg.nodeid()))
        {
            return;
        }

        auto llInstanceID = oPaxosMsg.instanceid();
        auto poInstance = GetInstanceByInstanceID(llInstanceID);
        if (poInstance)
        {
            poInstance->OnReceivePaxosMsg(oPaxosMsg);
        }
    }
    else if (iCmd == MsgCmd_CheckpointMsg)
    {
        CheckpointMsg oCheckpointMsg;
        bool bSucc = oCheckpointMsg.ParseFromArray(sBuffer.data() + iBodyStartPos, iBodyLen);
        if (!bSucc)
        {
            BP->GetInstanceBP()->OnReceiveParseError();
            PLGErr("PaxosMsg.ParseFromArray fail, skip this msg");
            return;
        }

        if (!ReceiveMsgHeaderCheck(oHeader, oCheckpointMsg.nodeid()))
        {
            return;
        }
        
        OnReceiveCheckpointMsg(oCheckpointMsg);
    }
}

void Group :: SetPromiseInfo(const uint64_t llPromiseInstanceID, const uint64_t llEndPromiseInstanceID)
{
    m_setPromiseInstanceID.insert(llPromiseInstanceID);
    if (m_setPromiseInstanceID.size() > GetMaxWindowSize())
    {
        m_setPromiseInstanceID.erase(m_setPromiseInstanceID.begin());
    }

    m_setEndPromiseInstanceID.insert(llEndPromiseInstanceID);
    if (m_setEndPromiseInstanceID.size() > GetMaxWindowSize())
    {
        m_setEndPromiseInstanceID.erase(m_setEndPromiseInstanceID.begin());
    }
}

bool Group :: NeedPrepare(const uint64_t llInstanceID)
{
    if (m_setEndPromiseInstanceID.end() == m_setEndPromiseInstanceID.find(llInstanceID))
    {
        return true;
    }

    auto it = m_setPromiseInstanceID.upper_bound(llInstanceID);
    if (m_setPromiseInstanceID.begin() == it)
    {
        return true;
    }

    return false;
}


void Group :: GetMaxInstanceIDFromLog(uint64_t & llMaxInstanceID)
{
    return m_oPaxosLog.GetMaxInstanceIDFromLog(m_poConfig->GetMyGroupIdx(), llMaxInstanceID);
}

int Group :: ProtectionLogic_IsCheckpointInstanceIDCorrect(const uint64_t llCPInstanceID, const uint64_t llLogMaxInstanceID) 
{
    if (llCPInstanceID <= llLogMaxInstanceID + 1)
    {
        return 0;
    }

    //checkpoint_instanceid larger than log_maxinstanceid+1 will appear in the following situations 
    //1. Pull checkpoint from other node automatically and restart. (normal case)
    //2. Paxos log was manually all deleted. (may be normal case)
    //3. Paxos log is lost because Options::bSync set as false. (bad case)
    //4. Checkpoint data corruption results an error checkpoint_instanceid. (bad case)
    //5. Checkpoint data copy from other node manually. (bad case)
    //In these bad cases, paxos log between [log_maxinstanceid, checkpoint_instanceid) will not exist
    //and checkpoint data maybe wrong, we can't ensure consistency in this case.

    if (llLogMaxInstanceID == 0)
    {
        //case 1. Automatically pull checkpoint will delete all paxos log first.
        //case 2. No paxos log. 
        //If minchosen instanceid < checkpoint instanceid.
        //Then Fix minchosen instanceid to avoid that paxos log between [log_maxinstanceid, checkpoint_instanceid) not exist.
        //if minchosen isntanceid > checkpoint.instanceid.
        //That probably because the automatic pull checkpoint did not complete successfully.
        uint64_t llMinChosenInstanceID = m_oCheckpointMgr.GetMinChosenInstanceID();
        if (m_oCheckpointMgr.GetMinChosenInstanceID() != llCPInstanceID)
        {
            int ret = m_oCheckpointMgr.SetMinChosenInstanceID(llCPInstanceID);
            if (ret != 0)
            {
                PLGErr("SetMinChosenInstanceID fail, now minchosen %lu max instanceid %lu checkpoint instanceid %lu",
                        m_oCheckpointMgr.GetMinChosenInstanceID(), llLogMaxInstanceID, llCPInstanceID);
                return -1;
            }

            PLGStatus("Fix minchonse instanceid ok, old minchosen %lu now minchosen %lu max %lu checkpoint %lu",
                    llMinChosenInstanceID, m_oCheckpointMgr.GetMinChosenInstanceID(),
                    llLogMaxInstanceID, llCPInstanceID);
        }

        return 0;
    }
    else
    {
        //other case.
        PLGErr("checkpoint instanceid %lu larger than log max instanceid %lu. "
                "Please ensure that your checkpoint data is correct. "
                "If you ensure that, just delete all paxos log data and restart.",
                llCPInstanceID, llLogMaxInstanceID);
        return -2;
    }
}

int Group :: PlayLog(const uint64_t llBeginInstanceID, const uint64_t llEndInstanceID)
{
    if (llBeginInstanceID < m_oCheckpointMgr.GetMinChosenInstanceID())
    {
        PLGErr("now instanceid %lu small than min chosen instanceid %lu", 
                llBeginInstanceID, m_oCheckpointMgr.GetMinChosenInstanceID());
        return -2;
    }

    for (uint64_t llInstanceID = llBeginInstanceID; llInstanceID < llEndInstanceID; llInstanceID++)
    {
        AcceptorStateData oState; 
        int ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
        if (ret != 0)
        {
            PLGErr("log read fail, instanceid %lu ret %d", llInstanceID, ret);
            return ret;
        }

        bool bExecuteRet = m_oSMFac.Execute(m_poConfig->GetMyGroupIdx(), llInstanceID, oState.acceptedvalue(), nullptr);
        if (!bExecuteRet)
        {
            PLGErr("Execute fail, instanceid %lu", llInstanceID);
            return -1;
        }
    }

    return 0;
}



}


