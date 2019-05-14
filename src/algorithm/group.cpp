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

using namespace std;

namespace phxpaxos
{


Group :: Group(LogStorage * poLogStorage, 
            NetWork * poNetWork,    
            InsideSM * poMasterSM,
            const int iGroupIdx,
            const Options & oOptions) : 
    m_oOptions(oOptions),
    m_poLogStorage(poLogStorage),
    m_oPaxosLog(poLogStorage),
    m_iMyGroupIdx(iGroupIdx),
    m_oConfig(poLogStorage, oOptions.bSync, oOptions.iSyncInterval, oOptions.bUseMembership, 
            oOptions.oMyNode, oOptions.vecNodeInfoList, oOptions.vecFollowerNodeInfoList, 
            iGroupIdx, oOptions.iGroupCount, oOptions.pMembershipChangeCallback),
    m_oCommunicate(&m_oConfig, oOptions.oMyNode.GetNodeID(), oOptions.iUDPMaxSize, poNetWork),
    m_oIOLoop(&m_oConfig, this),
    m_oSMFac(m_oConfig.GetMyGroupIdx()),
    m_oCommitter(&m_oConfig, &m_oIOLoop, &m_oSMFac),
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


int Group :: LoadMaxInstanceID(uint64_t & llInstanceID)
{
    // maybe it has max ballot
    int ret = GetMaxInstanceIDFromLog(llInstanceID);
    if (ret != 0 && ret != 1)
    {
        PLG1Err("Load max instance id fail, ret %d", ret);
        return ret;
    }

    if (ret == 1)
    {
        PLG1Err("empty database");
        llInstanceID = 0;
        return 0;
    }

    AcceptorStateData oState;
    ret = m_oPaxosLog.ReadState(m_iMyGroupIdx, llInstanceID, oState);
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

    PLG1Imp("GroupIdx %d InstanceID %lu PromiseID %lu PromiseNodeID %lu"
           " AccectpedID %lu AcceptedNodeID %lu ValueLen %zu Checksum %u", 
            m_iMyGroupIdx, llInstanceID,
            oState.promiseid(), oState.promisenodeid(),
            oState.acceptedid(), oState.acceptednodeid(), 
            oState.acceptedvalue().size(), oState.checksum());
    
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
    int ret = m_oPaxosLog.ReadState(m_iMyGroupIdx, m_llNowInstanceID - 1, oState);
    if (ret != 0 && ret != 1)
    {
        return ret;
    }

    if (ret == 1)
    {
        PLG1Err("las checksum not exist, now instanceid %lu", m_llNowInstanceID);
        m_iLastChecksum = 0;
        return 0;
    }

    m_iLastChecksum = oState.checksum();

    PLG1Imp("ok, last checksum %u", m_iLastChecksum);

    return 0;
}

void Group :: Init()
{

    PLG1Debug("(unix) Init");

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
    int m_iInitRet = LoadMaxInstanceID(llMaxInstanceID);
    if (m_iInitRet != 0 && m_iInitRet != 1)
    {
        PLG1Err("Load max instance id fail. iInitRet %d", m_iInitRet);
        return;
    }

    PLG1Debug("(unix) CPInstanceID %lu MaxInstanceID %lu", llCPInstanceID, llMaxInstanceID);

    if (m_iInitRet == 1)
    {
        PLG1Err("empty database");
        m_iInitRet = 0;
        llMaxInstanceID = 0;
    }

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

            PLG1Imp("PlayLog OK, begin instanceid %lu end instanceid %lu", m_llNowInstanceID, llMaxInstanceID);

            m_llNowInstanceID = llMaxInstanceID;
        }
        else
        {
            if (m_llNowInstanceID > llMaxInstanceID)
            {
                m_iInitRet = ProtectionLogic_IsCheckpointInstanceIDCorrect(m_llNowInstanceID, llMaxInstanceID);
                if (m_iInitRet != 0)
                {
                    return;
                }
                //m_oAcceptor.InitForNewPaxosInstance();
            }

            //m_oAcceptor.SetInstanceID(m_llNowInstanceID);
        }

        PLG1Imp("NowInstanceID %lu", m_llNowInstanceID);

        m_oLearner.SetInstanceID(m_llNowInstanceID);
        //m_oProposer.SetInstanceID(m_llNowInstanceID);

        m_oCheckpointMgr.SetMaxCommitInstanceID(m_llNowInstanceID);

        m_iInitRet = InitLastCheckSum();
        if (m_iInitRet != 0)
        {
            return;
        }

        m_oLearner.Reset_AskforLearn_Noop();

        PLG1Imp("OK");
    }


    m_llNowIdleInstanceID = m_llNowInstanceID;

    PLG1Debug("(unix) CPInstanceID %lu NowIdleInstanceID %lu", llCPInstanceID, m_llNowIdleInstanceID);

}

int Group :: GetInitRet()
{
    m_poThread->join();
    delete m_poThread;

    return m_iInitRet;
}

void Group :: Start()
{
    //start learner sender
    m_oLearner.StartLearnerSender();
    //start ioloop
    m_oIOLoop.start();
    //start checkpoint replayer and cleaner
    m_oCheckpointMgr.Start();

    m_bStarted = true;
}

void Group :: Stop()
{
    if (m_bStarted)
    {
        m_oIOLoop.Stop();
        m_oCheckpointMgr.Stop();
        m_oLearner.Stop();
    }
}

Config * Group :: GetConfig()
{
    return &m_oConfig;
}

Instance * Group :: GetInstance(uint64_t llInstanceID)
{
    auto it = m_mapInstances.find(llInstanceID);
    if (m_mapInstances.end() != it) {
        return it->second.get();
    }

    if (NoCheckpoint != m_llNowInstanceID && llInstanceID < m_llNowInstanceID) { // instance already removed, maybe old msg arrived
        PLG1Debug("(unix) InstanceID %lu < NowInstanceID %lu, ignore", llInstanceID, m_llNowInstanceID);
        return nullptr;
    }

    auto ret = m_mapInstances.insert(make_pair(llInstanceID, unique_ptr<Instance>(new Instance(&m_oConfig, m_poLogStorage, &m_oCommunicate, m_oOptions, this))));
    if (!ret.second) return nullptr;

    ret.first->second->Init(llInstanceID);
    return ret.first->second.get();
}

Committer * Group :: GetCommitter()
{
    return &m_oCommitter;
}

Cleaner * Group :: GetCheckpointCleaner()
{
    return m_oCheckpointMgr.GetCleaner();
}

Replayer * Group :: GetCheckpointReplayer()
{
    return m_oCheckpointMgr.GetReplayer();
}

uint64_t Group :: GetProposalID() const
{
    return m_llProposalID;
}

void Group :: NewPrepare()
{
    PLG1Head("START ProposalID %lu HighestOther %lu MyNodeID %lu",
            m_llProposalID, m_llHighestOtherProposalID, m_oConfig.GetMyNodeID());

    uint64_t llMaxProposalID =
        m_llProposalID > m_llHighestOtherProposalID ? m_llProposalID : m_llHighestOtherProposalID;

    m_llProposalID = llMaxProposalID + 1;

    PLG1Head("END New.ProposalID %lu", m_llProposalID);
}

Learner * Group :: GetLearner()
{
    return &m_oLearner;
}

SMFac * Group :: GetSMFac()
{
    return &m_oSMFac;
}


int Group :: OnReceiveMessage(const char * pcMessage, const int iMessageLen)
{
    m_oIOLoop.AddMessage(pcMessage, iMessageLen);

    return 0;
}

IOLoop * Group :: GetIOLoop()
{
    return &m_oIOLoop;
}

const Options * Group :: GetOptions()
{
    return &m_oOptions;
}

uint64_t Group :: GetNowInstanceID() const
{
    return m_llNowInstanceID;
}

uint64_t Group :: GetMinChosenInstanceID() const
{
    return m_oCheckpointMgr.GetMinChosenInstanceID();
}

int Group :: GetInstanceValue(const uint64_t llInstanceID, std::string & sValue, int & iSMID)
{
    iSMID = 0;

    AcceptorStateData oState; 
    int ret = m_oPaxosLog.ReadState(m_iMyGroupIdx, llInstanceID, oState);
    if (ret != 0 && ret != 1)
    {
        return -1;
    }

    if (ret == 1)
    {
        return Paxos_GetInstanceValue_Value_NotExist;
    }

    memcpy(&iSMID, oState.acceptedvalue().data(), sizeof(int));
    sValue = string(oState.acceptedvalue().data() + sizeof(int), oState.acceptedvalue().size() - sizeof(int));

    return 0;
}


void Group :: AddStateMachine(StateMachine * poSM)
{
    m_oSMFac.AddSM(poSM);
}

bool Group :: HasIdleInstance(uint64_t & llInstanceID)
{
    auto iWindowSize = m_oConfig.GetWindowSize();

    PLG1Debug("(unix) NowIdleInstanceID %lu NowInstanceID %lu WindowSize %u", m_llNowIdleInstanceID, m_llNowInstanceID, iWindowSize);

    llInstanceID = NoCheckpoint;
    if (NoCheckpoint == m_llNowInstanceID || NoCheckpoint == m_llNowIdleInstanceID) { // uninit
        return false;
    }
    if (m_llNowIdleInstanceID < m_llNowInstanceID) {
        m_llNowIdleInstanceID = m_llNowInstanceID; // fix m_llNowIdleInstanceID
    }
    if (m_llNowIdleInstanceID >= m_llNowInstanceID + iWindowSize) {
        return false;
    }

    llInstanceID = m_llNowIdleInstanceID;
    return true;
}

void Group :: AddTimeoutInstance(const uint64_t llInstaceID)
{
    m_seTimeoutInstnaceList.insert(llInstaceID);
}

bool Group :: HasTimeoutInstance(uint64_t & llInstanceID)
{
    llInstanceID = NoCheckpoint;

    if (m_seTimeoutInstnaceList.empty()) return false;

    llInstanceID = *m_seTimeoutInstnaceList.begin();
    m_seTimeoutInstnaceList.erase(m_seTimeoutInstnaceList.begin());

    return true;
}


int Group :: NewValue(const uint64_t llInstanceID, const std::string & sValue, shared_ptr<CommitCtx> poCommitCtx)
{
    auto poInstance = GetInstance(llInstanceID);
    if (!poInstance) {
        return -1;
    }

    if (poCommitCtx) {
        poInstance->SetCommitCtx(poCommitCtx);
    }

    return poInstance->NewValue(sValue);
}

void Group :: NewIdleInstance()
{
    ++m_llNowIdleInstanceID;
}

void Group :: SetOtherProposalID(const uint64_t llOtherProposalID)
{
    if (llOtherProposalID > m_llHighestOtherProposalID)
    {
        m_llHighestOtherProposalID = llOtherProposalID;
    }
}

void Group :: SetPromiseBallot(const uint64_t llInstanceID, const BallotNumber &oBallotNumber)
{
    uint64_t llEndPromiseInstanceID{NoCheckpoint};
    BallotNumber oPromiseBallotNumber = GetPromiseBallot(llInstanceID, llEndPromiseInstanceID);

    if (!(oBallotNumber > oPromiseBallotNumber)) return;

    m_mapInstanceID2PromiseBallot[llInstanceID] = oBallotNumber;

    PLG1Debug("(unix) set new PromiseBallot(ProposalID: %lu, NodeID: %lu). InstanceID %lu", oBallotNumber.m_llProposalID, oBallotNumber.m_llNodeID, llInstanceID);

    while (m_mapInstanceID2PromiseBallot.size() > m_oConfig.GetMaxWindowSize())
    {
        m_mapInstanceID2PromiseBallot.erase(m_mapInstanceID2PromiseBallot.begin());
    }
}

BallotNumber Group :: GetPromiseBallot(const uint64_t llInstanceID, uint64_t & llEndPromiseInstanceID) const
{
    llEndPromiseInstanceID = NoCheckpoint;

    auto it = m_mapInstanceID2PromiseBallot.upper_bound(llInstanceID);
    if (m_mapInstanceID2PromiseBallot.end() != it) {
        llEndPromiseInstanceID = it->first;
    }
    if (m_mapInstanceID2PromiseBallot.begin() == it) {
        PLG1Debug("(unix) PromiseBallot empty. InstanceID %lu EndPromiseInstanceID %lu", llInstanceID, llEndPromiseInstanceID);
        return BallotNumber();
    }

    --it;

    PLG1Debug("(unix) PromiseBallot(ProposalID: %lu, NodeID: %lu). InstanceID %lu EndPromiseInstanceID %lu", it->second.m_llProposalID, it->second.m_llNodeID, llInstanceID, llEndPromiseInstanceID);

    return it->second;
}

void Group :: OnReceiveCheckpointMsg(const CheckpointMsg & oCheckpointMsg)
{
    PLG1Imp("Now.InstanceID %lu MsgType %d Msg.from_nodeid %lu My.nodeid %lu flag %d"
            " uuid %lu sequence %lu checksum %lu offset %lu buffsize %zu filepath %s",
            m_llNowInstanceID, oCheckpointMsg.msgtype(), oCheckpointMsg.nodeid(),
            m_oConfig.GetMyNodeID(), oCheckpointMsg.flag(), oCheckpointMsg.uuid(), oCheckpointMsg.sequence(), oCheckpointMsg.checksum(),
            oCheckpointMsg.offset(), oCheckpointMsg.buffer().size(), oCheckpointMsg.filepath().c_str());

    if (oCheckpointMsg.msgtype() == CheckpointMsgType_SendFile)
    {
        if (!m_oCheckpointMgr.InAskforcheckpointMode())
        {
            PLG1Imp("not in ask for checkpoint mode, ignord checkpoint msg");
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
        PLG1Err("Header check fail, header.gid %lu config.gid %lu, msg.from_nodeid %lu",
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
        PLG1Err("buffer size %zu too short", sBuffer.size());
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
            PLG1Imp("in ask for checkpoint mode, ignord paxosmsg");
            return;
        }
        
        PaxosMsg oPaxosMsg;
        bool bSucc = oPaxosMsg.ParseFromArray(sBuffer.data() + iBodyStartPos, iBodyLen);
        if (!bSucc)
        {
            BP->GetInstanceBP()->OnReceiveParseError();
            PLG1Err("PaxosMsg.ParseFromArray fail, skip this msg");
            return;
        }

        if (!ReceiveMsgHeaderCheck(oHeader, oPaxosMsg.nodeid()))
        {
            PLG1Err("ReceiveMsgHeaderCheck fail, skip this msg. Config.GetGid %d Header.gid %d PaxosMsg.nodeid %d", m_oConfig.GetGid(), oHeader.gid(), oPaxosMsg.nodeid());
            return;
        }

        auto llInstanceID = oPaxosMsg.instanceid();
        auto poInstance = GetInstance(llInstanceID);
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
            PLG1Err("PaxosMsg.ParseFromArray fail, skip this msg");
            return;
        }

        if (!ReceiveMsgHeaderCheck(oHeader, oCheckpointMsg.nodeid()))
        {
            return;
        }
        
        OnReceiveCheckpointMsg(oCheckpointMsg);
    }
}


void Group :: ReceiveMsgForLearner(const PaxosMsg & oPaxosMsg)
{
    if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_AskforLearn)
    {
        m_oLearner.OnAskforLearn(oPaxosMsg);
    }
    else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendLearnValue)
    {
        m_oLearner.OnSendLearnValue(oPaxosMsg);
    }
    else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_ProposerSendSuccess)
    {
        m_oLearner.OnProposerSendSuccess(oPaxosMsg);
    }
    else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendNowInstanceID)
    {
        m_oLearner.OnSendNowInstanceID(oPaxosMsg);
    }
    else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_ComfirmAskforLearn)
    {
        m_oLearner.OnComfirmAskForLearn(oPaxosMsg);
    }
    else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendLearnValue_Ack)
    {
        m_oLearner.OnSendLearnValue_Ack(oPaxosMsg);
    }
    else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_AskforCheckpoint)
    {
        m_oLearner.OnAskforCheckpoint(oPaxosMsg);
    }

    ProcessCommit();

}


void Group :: ProcessCommit()
{
    PLG1Debug("(unix) begin");

    uint64_t llInstanceID{NoCheckpoint};
    std::string sValue;
    while (m_oLearner.GetPendingCommit(llInstanceID, sValue)) {

        PLG1Debug("(unix) pending commit. InstanceID %lu", llInstanceID);

        BP->GetInstanceBP()->OnInstanceLearned();

        SMCtx * poSMCtx = nullptr;

        auto poInstance = GetInstance(llInstanceID);
        if (!poInstance)
        {
            PLG1Err("poInstance null, instanceid %lu", llInstanceID);
            return;
        }

        auto poCommitCtx = poInstance->GetCommitCtx();
        if (poCommitCtx)
        {
            PLG1Debug("(unix) CommitCtx exist");

            bool bIsMyCommit = poCommitCtx->IsMyCommit(llInstanceID, sValue, poSMCtx);

            if (!bIsMyCommit)
            {
                BP->GetInstanceBP()->OnInstanceLearnedNotMyCommit();
                PLG1Debug("this value is not my commit");
            }
            else
            {
                auto iUseTimeMs = 0; // TODO
                BP->GetInstanceBP()->OnInstanceLearnedIsMyCommit(iUseTimeMs);
                PLG1Head("My commit ok, usetime %dms", iUseTimeMs);
            }
        } else {
            PLG1Debug("(unix) CommitCtx not exist");
        }


        if (!m_oSMFac.Execute(m_iMyGroupIdx, llInstanceID, sValue, poSMCtx))
        {
            BP->GetInstanceBP()->OnInstanceLearnedSMExecuteFail();

            PLG1Err("SMExecute fail, instanceid %lu, not increase instanceid", llInstanceID);
            if (poCommitCtx)
            {
                poCommitCtx->SetResult(PaxosTryCommitRet_ExecuteFail, 
                                         llInstanceID, sValue);
            }

            //m_oProposer.CancelSkipPrepare();

            return;
        }
        
        {
            if (poCommitCtx)
            {
                PLG1Debug("(unix) CommitTimerID %d", poCommitCtx->GetCommitTimerID());

                poCommitCtx->SetResult(PaxosTryCommitRet_OK,
                                         llInstanceID, sValue);

                if (poCommitCtx->GetCommitTimerID() > 0)
                {
                    auto iTimerID = poCommitCtx->GetCommitTimerID();
                    m_oIOLoop.RemoveTimer(iTimerID);
                    poCommitCtx->SetCommitTimerID(iTimerID);
                }
            }

            //this paxos instance end, tell proposal done



        }
        
        PLG1Head("[Learned] learned instanceid %lu. New paxos starting", llInstanceID);

        m_oCheckpointMgr.SetMaxCommitInstanceID(llInstanceID);

        if (!m_oLearner.FinishCommit(llInstanceID))
        {
            PLG1Err("FinishCommit fail, instanceid %lu", llInstanceID);
            return;
        }

        // increase nowinstanceid
        if (llInstanceID + 1 > m_llNowInstanceID) {
            m_llNowInstanceID = llInstanceID + 1;
        }
        PLG1Head("[Learned] NowInstanceID increase to %lu", m_llNowInstanceID);
        while (!m_mapInstances.empty() && m_mapInstances.begin()->first < m_llNowInstanceID) {
            PLG1Debug("(unix) erase instance %lu", m_mapInstances.begin()->first);
            m_mapInstances.erase(m_mapInstances.begin());
        }
    }
}


bool Group :: NeedPrepare(const uint64_t llInstanceID)
{
    BallotNumber oMyBallotNumber(m_llProposalID, m_oConfig.GetMyNodeID());

    uint64_t llEndPromiseInstanceID{NoCheckpoint};
    BallotNumber oPromiseBallotNumber = GetPromiseBallot(llInstanceID, llEndPromiseInstanceID);

    if (oPromiseBallotNumber.isnull()) {
        return true;
    }

    return oPromiseBallotNumber > oMyBallotNumber;

}


int Group :: GetMaxInstanceIDFromLog(uint64_t & llMaxInstanceID)
{
    return m_oPaxosLog.GetMaxInstanceIDFromLog(m_iMyGroupIdx, llMaxInstanceID);
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
                PLG1Err("SetMinChosenInstanceID fail, now minchosen %lu max instanceid %lu checkpoint instanceid %lu",
                        m_oCheckpointMgr.GetMinChosenInstanceID(), llLogMaxInstanceID, llCPInstanceID);
                return -1;
            }

            PLG1Status("Fix minchonse instanceid ok, old minchosen %lu now minchosen %lu max %lu checkpoint %lu",
                    llMinChosenInstanceID, m_oCheckpointMgr.GetMinChosenInstanceID(),
                    llLogMaxInstanceID, llCPInstanceID);
        }

        return 0;
    }
    else
    {
        //other case.
        PLG1Err("checkpoint instanceid %lu larger than log max instanceid %lu. "
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
        PLG1Err("now instanceid %lu small than min chosen instanceid %lu", 
                llBeginInstanceID, m_oCheckpointMgr.GetMinChosenInstanceID());
        return -2;
    }

    for (uint64_t llInstanceID = llBeginInstanceID; llInstanceID < llEndInstanceID; llInstanceID++)
    {
        AcceptorStateData oState; 
        int ret = m_oPaxosLog.ReadState(m_iMyGroupIdx, llInstanceID, oState);
        if (ret != 0)
        {
            PLG1Err("log read fail, instanceid %lu ret %d", llInstanceID, ret);
            return ret;
        }

        bool bExecuteRet = m_oSMFac.Execute(m_oConfig.GetMyGroupIdx(), llInstanceID, oState.acceptedvalue(), nullptr);
        if (!bExecuteRet)
        {
            PLG1Err("Execute fail, instanceid %lu", llInstanceID);
            return -1;
        }
    }

    return 0;
}



}


