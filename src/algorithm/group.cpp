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
            SoftState * poSoftState,
            const int iGroupIdx,
            const Options & oOptions) : 
    m_oOptions(oOptions),
    m_poLogStorage(poLogStorage),
    m_oPaxosLog(poLogStorage),
    m_iMyGroupIdx(iGroupIdx),
    m_oConfig(poLogStorage, oOptions.bSync, oOptions.iSyncInterval, oOptions.bUseMembership, 
            oOptions.oMyNode, oOptions.vecNodeInfoList, oOptions.vecFollowerNodeInfoList, 
            iGroupIdx, oOptions.iGroupCount, oOptions.pMembershipChangeCallback, oOptions.iMaxWindowSize),
    m_oCommunicate(&m_oConfig, oOptions.oMyNode.GetNodeID(), oOptions.iUDPMaxSize, poNetWork),
    m_oIOLoop(&m_oConfig, this),
    m_oSMFac(m_oConfig.GetMyGroupIdx()),
    m_oCommitter(&m_oConfig, &m_oIOLoop, &m_oSMFac),
    m_oCheckpointMgr(&m_oConfig, &m_oSMFac, poLogStorage, oOptions.bUseCheckpointReplayer),
    m_oLearner(&m_oConfig, &m_oCommunicate, this, poLogStorage, &m_oIOLoop, &m_oCheckpointMgr, &m_oSMFac),
    m_iInitRet(-1), m_poThread(nullptr),
    m_poSoftState(poSoftState)
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
/*
    AcceptorStateData oState;
    ret = m_oPaxosLog.ReadState(m_iMyGroupIdx, llInstanceID, oState);
    if (ret != 0)
    {
        return ret;
    }

    m_llProposalID = oState.promiseid(); // TODO(unix): the max proposalid should be maintained by softstate
*/
/*
    // m_mapInstanceID2PromiseBallot should be maintained by softstate
    m_mapInstanceID2PromiseBallot.clear();
    m_mapInstanceID2PromiseBallot[llInstanceID] = BallotNumber(oState.promiseid(), oState.promisenodeid());
*/

   /*

    m_oPromiseBallot.m_llProposalID = oState.promiseid();
    m_oPromiseBallot.m_llNodeID = oState.promisenodeid();
    m_oAcceptedBallot.m_llProposalID = oState.acceptedid();
    m_oAcceptedBallot.m_llNodeID = oState.acceptednodeid();
    m_sAcceptedValue = oState.acceptedvalue();
    m_iChecksum = oState.checksum();
    */
/*
    PLG1Imp("GroupIdx %d InstanceID %lu PromiseID %lu PromiseNodeID %lu"
           " AccectpedID %lu AcceptedNodeID %lu ValueLen %zu Checksum %u", 
            m_iMyGroupIdx, llInstanceID,
            oState.promiseid(), oState.promisenodeid(),
            oState.acceptedid(), oState.acceptednodeid(), 
            oState.acceptedvalue().size(), oState.checksum());
*/
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

    m_oCheckpointMgr.SetMinChosenInstanceIDUpdateCallbackFunc([&](const uint64_t llMinChosenInstanceID) {
                                                                m_poSoftState->OnMinChosenInstanceIDUpdate(llMinChosenInstanceID);
                                                              });

    //inside sm
    AddStateMachine(m_oConfig.GetSystemVSM());
    AddStateMachine(m_oConfig.GetMasterSM());
    
    // load minchoseninstanceid
    uint64_t llMinChosenInstanceID = m_oCheckpointMgr.GetMinChosenInstanceID();

    //load cp instanceid
    uint64_t llCPInstanceID = m_oCheckpointMgr.GetCheckpointInstanceID();

    //load max instanceid
    uint64_t llMaxInstanceID{0};
    int m_iInitRet = LoadMaxInstanceID(llMaxInstanceID);
    if (m_iInitRet != 0 && m_iInitRet != 1)
    {
        PLG1Err("Load max instance id fail. iInitRet %d", m_iInitRet);
        return;
    }

    PLG1Debug("(unix) MinChosenInstanceID %lu CPInstanceID %lu MaxInstanceID %lu", llMinChosenInstanceID, llCPInstanceID, llMaxInstanceID);

    if (m_iInitRet == 1)
    {
        PLG1Err("empty database");
        m_iInitRet = 0;
        llMaxInstanceID = 0;
    }

    // init nowinstanceid
    {
      m_llNowInstanceID = 0;
      if (llMaxInstanceID >= 100) {
        m_llNowInstanceID = llMaxInstanceID - 100;
      }
      PLG1Debug("(unix) MaxInstanceID %lu NowInstanceID %lu", llMaxInstanceID, m_llNowInstanceID);
    }

    // check cp
    {
      m_iInitRet = ProtectionLogic_IsCheckpointInstanceIDCorrect(llCPInstanceID, llMaxInstanceID);
      if (m_iInitRet != 0)
      {
        return;
      }
    }

    // rebuild softstate
    {
      RebuildSoftState(llMinChosenInstanceID, llMaxInstanceID);
    }

    // rebuild instance
    {
      uint64_t llBeginInstanceID = m_llNowInstanceID;
      uint64_t llEndInstanceID = llMaxInstanceID;

        // play [llBeginInstanceID, llEndInstanceID)
        if (llBeginInstanceID <= llEndInstanceID)
        {
            m_iInitRet = RebuildInstance(llBeginInstanceID, llEndInstanceID);
            if (m_iInitRet != 0)
            {
                return;
            }

            PLG1Imp("RebuildInstace OK, begin instanceid %lu end instanceid %lu", m_llNowInstanceID, llMaxInstanceID);
        }
    }


    //playlog
    {
        uint64_t llBeginInstanceID = llCPInstanceID + 1;
        uint64_t llEndInstanceID = m_llNowInstanceID;

        // play [llBeginInstanceID, llEndInstanceID)
        if (llBeginInstanceID < llEndInstanceID)
        {
            m_iInitRet = PlayLog(llBeginInstanceID, llEndInstanceID);
            if (m_iInitRet != 0)
            {
                return;
            }

            PLG1Imp("PlayLog OK, begin instanceid %lu end instanceid %lu", m_llNowInstanceID, llMaxInstanceID);
        }

        PLG1Imp("NowInstanceID %lu", m_llNowInstanceID);

        m_oLearner.SetInstanceID(m_llNowInstanceID);
        //m_oProposer.SetInstanceID(m_llNowInstanceID);

        m_oCheckpointMgr.SetNowInstanceID(m_llNowInstanceID);

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

Learner * Group :: GetLearner()
{
    return &m_oLearner;
}

SMFac * Group :: GetSMFac()
{
    return &m_oSMFac;
}

SoftState * Group :: GetSoftState() {
  return m_poSoftState;
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
            PLG1Err("(unix)ReceiveMsgHeaderCheck fail, skip this msg. Config.GetGid %d Header.gid %d PaxosMsg.nodeid %d", m_oConfig.GetGid(), oHeader.gid(), oPaxosMsg.nodeid());
            return;
        }

        PLG1Debug("(unix)Msg.InstanceID %lu MsgType %d Msg.from_nodeid %lu My.nodeid %lu",
                  oPaxosMsg.instanceid(), oPaxosMsg.msgtype(),
                  oPaxosMsg.nodeid(), m_oConfig.GetMyNodeID());

        OnReceivePaxosMsg(oPaxosMsg);

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


int Group :: OnReceivePaxosMsg(const PaxosMsg & oPaxosMsg, const bool bIsRetry) {
    if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_AskforLearn
        || oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendLearnValue
        || oPaxosMsg.msgtype() == MsgType_PaxosLearner_ProposerSendSuccess
        || oPaxosMsg.msgtype() == MsgType_PaxosLearner_ComfirmAskforLearn
        || oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendNowInstanceID
        || oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendLearnValue_Ack
        || oPaxosMsg.msgtype() == MsgType_PaxosLearner_AskforCheckpoint)
    {
        //ChecksumLogic(oPaxosMsg);
        ReceiveMsgForLearner(oPaxosMsg);
    } else {
        auto llInstanceID = oPaxosMsg.instanceid();
        auto poInstance = GetInstance(llInstanceID);
        if (!poInstance) return -1;
        return poInstance->OnReceivePaxosMsg(oPaxosMsg, bIsRetry);
    }
    return 0;
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

    std::shared_ptr<LearnerState::LearnState> poLearnState{nullptr};
    while (poLearnState = m_oLearner.GetPendingCommit()) {

        m_poSoftState->UpdateOnCommit(poLearnState->llInstanceID, poLearnState->sValue);
        auto iMyLastChecksum = m_poSoftState->GetLastChecksum(poLearnState->llInstanceID);
        PLG1Debug("(unix) InstanceID %lu local LastChecksum %u peer LastChecksum %u", poLearnState->llInstanceID, iMyLastChecksum, poLearnState->iLastChecksum);
        if (poLearnState->iLastChecksum && iMyLastChecksum) { // need check
          if (poLearnState->iLastChecksum != iMyLastChecksum) {
            PLG1Err("checksum fail, InstanceID %lu my last checksum %u other last checksum %u", 
                    poLearnState->llInstanceID, iMyLastChecksum, poLearnState->iLastChecksum);
            //BP->GetInstanceBP()->ChecksumLogicFail(); // TODO
            assert(poLearnState->iLastChecksum == iMyLastChecksum);
          }
        }

        PLG1Debug("(unix) pending commit. InstanceID %lu", poLearnState->llInstanceID);

        BP->GetInstanceBP()->OnInstanceLearned();

        SMCtx * poSMCtx = nullptr;

        auto poInstance = GetInstance(poLearnState->llInstanceID);
        if (!poInstance)
        {
            PLG1Err("poInstance null, instanceid %lu", poLearnState->llInstanceID);
            return;
        }

        auto poCommitCtx = poInstance->GetCommitCtx();
        if (poCommitCtx)
        {
            PLG1Debug("(unix) CommitCtx exist. val %s", poLearnState->sValue.c_str()); // TODO(unix): remove value

            bool bIsMyCommit = poCommitCtx->IsMyCommit(poLearnState->llInstanceID, poLearnState->sValue, poSMCtx);

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


        if (!m_oSMFac.Execute(m_iMyGroupIdx, poLearnState->llInstanceID, poLearnState->sValue, poSMCtx))
        {
            BP->GetInstanceBP()->OnInstanceLearnedSMExecuteFail();

            PLG1Err("SMExecute fail, instanceid %lu, not increase instanceid", poLearnState->llInstanceID);
            if (poCommitCtx)
            {
                poCommitCtx->SetResult(PaxosTryCommitRet_ExecuteFail, 
                                         poLearnState->llInstanceID, poLearnState->sValue);
            }

            //m_oProposer.CancelSkipPrepare();

            return;
        }
        
        {
            if (poCommitCtx)
            {
                PLG1Debug("(unix) CommitTimerID %d", poCommitCtx->GetCommitTimerID());

                poCommitCtx->SetResult(PaxosTryCommitRet_OK,
                                         poLearnState->llInstanceID, poLearnState->sValue);

                if (poCommitCtx->GetCommitTimerID() > 0)
                {
                    auto iTimerID = poCommitCtx->GetCommitTimerID();
                    m_oIOLoop.RemoveTimer(iTimerID);
                    poCommitCtx->SetCommitTimerID(iTimerID);
                }
            }

            //this paxos instance end, tell proposal done



        }
        
        PLG1Head("[Learned] learned instanceid %lu. New paxos starting", poLearnState->llInstanceID);




        bool bNeedBroadcast = (m_oConfig.GetMyNodeID() == poLearnState->oBallot.m_llNodeID);
        if (!m_oLearner.FinishCommit(poLearnState->llInstanceID, bNeedBroadcast))
        {
            PLG1Err("FinishCommit fail, instanceid %lu", poLearnState->llInstanceID);
            return;
        }

        // increase nowinstanceid
        if (poLearnState->llInstanceID + 1 > m_llNowInstanceID) {
            m_llNowInstanceID = poLearnState->llInstanceID + 1;
            m_oCheckpointMgr.SetNowInstanceID(m_llNowInstanceID);
        }
        PLG1Head("[Learned] NowInstanceID increase to %lu", m_llNowInstanceID);

        // keep instances in window.
        // prevent instance from being destruct while calling Learner :: ProposerSendSuccess in Proposer :: OnAcceptReply
        while (!m_mapInstances.empty() && m_mapInstances.begin()->first + m_oConfig.GetMaxWindowSize() < m_llNowInstanceID) {
            PLG1Debug("(unix) erase instance %lu", m_mapInstances.begin()->first);
            m_mapInstances.erase(m_mapInstances.begin());
        }
    }
}


int Group :: GetMaxInstanceIDFromLog(uint64_t & llMaxInstanceID)
{
    return m_oPaxosLog.GetMaxInstanceIDFromLog(m_iMyGroupIdx, llMaxInstanceID);
}

int Group :: ProtectionLogic_IsCheckpointInstanceIDCorrect(const uint64_t llCPInstanceID, const uint64_t llLogMaxInstanceID) 
{
    if (-1 == llCPInstanceID || llCPInstanceID <= llLogMaxInstanceID)
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
        if (m_oCheckpointMgr.GetMinChosenInstanceID() != llCPInstanceID + 1)
        {
            int ret = m_oCheckpointMgr.SetMinChosenInstanceID(llCPInstanceID + 1);
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

int Group :: RebuildInstance(const uint64_t llBeginInstanceID, const uint64_t llEndInstanceID)
{
  if (llBeginInstanceID < m_oCheckpointMgr.GetMinChosenInstanceID())
  {
    PLG1Err("now instanceid %lu small than min chosen instanceid %lu", 
            llBeginInstanceID, m_oCheckpointMgr.GetMinChosenInstanceID());
    return -2;
  }

  for (uint64_t llInstanceID = llBeginInstanceID; llInstanceID <= llEndInstanceID; llInstanceID++)
  {
    AcceptorStateData oState;
    int ret = m_oPaxosLog.ReadState(m_iMyGroupIdx, llInstanceID, oState);
    if (ret != 0)
    {
      PLG1Err("log read fail, instanceid %lu ret %d", llInstanceID, ret);
      continue;
    }

    auto poInstance = GetInstance(llInstanceID);
    if (nullptr == poInstance) {
      PLG1Err("GetInstance fail, instanceid %lu", llInstanceID);
      return -2;
    }

    auto poAcceptor = poInstance->GetAcceptor();
    if (nullptr == poAcceptor) {
      PLG1Err("GetAcceptor fail, instanceid %lu", llInstanceID);
      return -2;
    }

    auto poAcceptorState = poAcceptor->GetAcceptorState();
    if (nullptr == poAcceptorState) {
      PLG1Err("GetAcceptorState, instanceid %lu", llInstanceID);
      return -2;
    }

    BallotNumber oAcceptedBallot(oState.acceptedid(), oState.acceptednodeid());
    poAcceptorState->SetAcceptedBallot(oAcceptedBallot);
    poAcceptorState->SetAcceptedValue(oState.acceptedvalue());

    PLG1Debug("(unix) rebuild Instance ok. InstanceID %lu value %s", llInstanceID, oState.acceptedvalue().c_str() + sizeof(int));
  }


  return 0;
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

        m_poSoftState->UpdateOnCommit(llInstanceID, oState.acceptedvalue());
    }

    return 0;
}

int Group::RebuildSoftState(const uint64_t llMinChosenInstanceID, const uint64_t llMaxInstanceID) {
  PLG1Debug("(unix) MinChosenInstanceID %lu MaxInstanceID %lu", llMinChosenInstanceID, llMaxInstanceID);

  int ret;
  for (uint64_t llInstanceID = llMinChosenInstanceID; llInstanceID <= llMaxInstanceID; ++llInstanceID) {
    AcceptorStateData oState;
    ret = m_oPaxosLog.ReadState(m_iMyGroupIdx, llInstanceID, oState);
    if (ret != 0)
    {
      continue;
    }
    m_poSoftState->UpdateOnPersist(llInstanceID, oState);
  }
  return 0;
}



}


