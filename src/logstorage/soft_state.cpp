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

#include "soft_state.h"

#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "crc32.h"

#include "db.h"
#include "paxos_msg.pb.h"

namespace phxpaxos
{

SoftState::SoftState(const Options & oOptions, const int iGroupIdx)
  : m_iMyGroupIdx(iGroupIdx), m_iMaxWindowSize(oOptions.iMaxWindowSize) {
}

void SoftState :: Update(const uint64_t llInstanceID, const AcceptorStateData &oState) {
  BallotNumber oPromiseBallot(oState.promiseid(), oState.promisenodeid());
  SetPromiseBallot(llInstanceID, oPromiseBallot);

  BallotNumber oAcceptedBallot(oState.acceptedid(), oState.acceptednodeid());
  if (!oAcceptedBallot.isnull()) {
    UpdateContinuousChecksum(oState.instanceid(), oState.acceptedvalue());
  }
}

void SoftState :: SetPromiseBallot(const uint64_t llInstanceID, const BallotNumber &oBallotNumber)
{
  uint64_t llEndPromiseInstanceID{NoCheckpoint};
  BallotNumber oPromiseBallotNumber = GetPromiseBallot(llInstanceID, llEndPromiseInstanceID);

  if (!(oBallotNumber > oPromiseBallotNumber)) return;

  m_mapInstanceID2PromiseBallot[llInstanceID] = oBallotNumber;

  PLG1Debug("(unix) set new PromiseBallot(ProposalID: %lu, NodeID: %lu). InstanceID %lu", oBallotNumber.m_llProposalID, oBallotNumber.m_llNodeID, llInstanceID);

  while (m_mapInstanceID2PromiseBallot.size() > m_iMaxWindowSize)
  {
    m_mapInstanceID2PromiseBallot.erase(m_mapInstanceID2PromiseBallot.begin());
  }
}

BallotNumber SoftState :: GetPromiseBallot(const uint64_t llInstanceID, uint64_t & llEndPromiseInstanceID) const
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


uint32_t SoftState::GetContinuousChecksum(const uint64_t llInstanceID) {
  while (m_llNextContinuousInstanceID <= llInstanceID) {
    uint32_t iLastContinuousChecksum{0};
    if (m_llNextContinuousInstanceID) {
      iLastContinuousChecksum = m_mapInstanceID2ContinuousChecksum[m_llNextContinuousInstanceID - 1];
    }
    auto iCheckSum = m_mapInstanceID2Checksum[m_llNextContinuousInstanceID];
    m_mapInstanceID2ContinuousChecksum[m_llNextContinuousInstanceID++] = crc32(iLastContinuousChecksum, (const uint8_t *)&iLastContinuousChecksum, sizeof(uint32_t), CRC32SKIP);
  }
  PLG1Debug("(unix) InstanceID %lu ContinueChecksum %u", llInstanceID, m_mapInstanceID2ContinuousChecksum[llInstanceID]);
  return m_mapInstanceID2ContinuousChecksum[llInstanceID];
}


void SoftState::UpdateContinuousChecksum(const uint64_t llInstanceID, const std::string &sAcceptedValue) {
  iChecksum = crc32(0, (const uint8_t *)sAcceptedvalue.data(), sAcceptedvalue.size(), CRC32SKIP);

  auto &&it = m_mapInstanceID2Checksum.find(llInstanceID);
  if (m_mapInstanceID2Checksum.end() != it && it->second == iChecksum) {
    return;
  }

  m_mapInstanceID2Checksum[llInstanceID] = iChecksum;
  if (llInstanceID < m_llNextContinuousInstanceID) {
    m_llNextContinuousInstanceID = llInstanceID;
  }

  while (m_mapInstanceID2Checksum.size() > m_iMaxWindowSize)
  {
    auto llEraseInstanceID = m_mapInstanceID2Checksum.begin()->first;

    m_mapInstanceID2Checksum.erase(llEraseInstanceID);
    m_mapInstanceID2ContinuousChecksum.erase(llEraseInstanceID);
  }

}

//////////////////////////////////////////////////////


int MultiSoftState::Init(const Options & oOptions) {
  for (int iGroupIdx = 0; iGroupIdx < oOptions.iGroupCount; iGroupIdx++) {
    PLDebug("(unix) iGroupIdx %d", iGroupIdx);
    m_vecSoftStateList.push_back(SoftState(oOptions, iGroupIdx));
  }

  return 0;
}

SoftState *MultiSoftState::GetSoftState(const int iGroupIdx) {
  if (iGroupIdx >= m_vecSoftStateList.size()) {
    PLErr("iGroupIdx %d < size %u", iGroupIdx, m_vecSoftStateList.size());
    return nullptr;
  }
  return &m_vecSoftStateList[iGroupIdx];
}


}
