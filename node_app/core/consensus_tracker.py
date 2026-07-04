import asyncio
import logging
from collections import defaultdict
from node_app.schemas.transaction import TransactionEnvelope


class ConsensusTracker:
    def __init__(self, node_count: int):
        self.lock = asyncio.Lock()
        self.node_count = node_count
        self.max_faulty = (node_count - 1) // 3
        self.quorum_threshold = 2 * self.max_faulty + 1
        self.local_head: int = 0
        self.pre_prepares: dict[int, TransactionEnvelope] = {}
        self.prepare_votes: dict[int, set[str]] = defaultdict(set)
        self.commit_votes: dict[int, set[str]] = defaultdict(set)
        self.local_stage: dict[int, str] = {}

    def get_next_expected_sequence(self) -> int:
        return self.local_head + 1

    async def add_pre_prepare(
        self, sequence_num: int, envelope: TransactionEnvelope
    ) -> bool:
        async with self.lock:
            if sequence_num != self.get_next_expected_sequence():
                return False
            self.pre_prepares[sequence_num] = envelope
            self.local_stage[sequence_num] = "PRE_PREPARED"
            return True

    async def add_prepare_vote(
        self, sequence_num: int, validator_identity: str
    ) -> tuple[bool, TransactionEnvelope | None]:
        async with self.lock:
            self.prepare_votes[sequence_num].add(validator_identity)
            
            if len(self.prepare_votes[sequence_num]) < self.quorum_threshold:
                return False, None
                
            if self.local_stage.get(sequence_num) != "PRE_PREPARED":
                return False, None

            self.local_stage[sequence_num] = "PREPARED"
            return True, self.pre_prepares.get(sequence_num)

    async def add_commit_vote(
        self, sequence_num: int, validator_identity: str
    ) -> tuple[bool, TransactionEnvelope | None]:
        async with self.lock:
            self.commit_votes[sequence_num].add(validator_identity)
            
            if len(self.commit_votes[sequence_num]) < self.quorum_threshold:
                return False, None
                
            if self.local_stage.get(sequence_num) != "PREPARED":
                return False, None

            self.local_stage[sequence_num] = "COMMITTED"
            self.local_head = sequence_num
            return True, self.pre_prepares.get(sequence_num)

    async def force_garbage_collection(self, sequence_num: int):
        async with self.lock:
            if self.local_stage.get(sequence_num) == "COMMITTED":
                return

            logging.warning(
                f"Consensus stalled at slot {sequence_num}. Evicting poisoned cache."
            )
            self.pre_prepares.pop(sequence_num, None)
            self.prepare_votes.pop(sequence_num, None)
            self.commit_votes.pop(sequence_num, None)
            self.local_head = sequence_num
            self.local_stage[sequence_num] = "EVICTED"

    async def has_pre_prepare(self, sequence_num: int) -> bool:
        async with self.lock:
            return sequence_num in self.pre_prepares