import asyncio
from collections import defaultdict
from node_app.schemas.transaction import TransactionEnvelope


class ConsensusTracker:
    def __init__(self, n: int):
        self.lock = asyncio.Lock()
        self.n = n
        self.f = (n - 1) // 3
        self.quorum = 2 * self.f + 1
        self.local_head: int = 0
        self.pre_prepares: dict[int, TransactionEnvelope] = {}
        self.prepare_votes: dict[int, set[str]] = defaultdict(set)
        self.commit_votes: dict[int, set[str]] = defaultdict(set)
        self.local_stage: dict[int, str] = {}

    def next_sequence(self) -> int:
        return self.local_head + 1

    async def add_pre_prepare(
        self, seq: int, envelope: TransactionEnvelope
    ) -> bool:
        async with self.lock:
            if seq != self.next_sequence():
                return False
            self.pre_prepares[seq] = envelope
            self.local_stage[seq] = "PRE_PREPARED"
            return True

    async def add_prepare_vote(
        self, seq: int, validator: str
    ) -> tuple[bool, TransactionEnvelope | None]:
        async with self.lock:
            self.prepare_votes[seq].add(validator)
            if (
                len(self.prepare_votes[seq]) >= self.quorum
                and self.local_stage.get(seq) == "PRE_PREPARED"
            ):
                self.local_stage[seq] = "PREPARED"
                return True, self.pre_prepares.get(seq)
            return False, None

    async def add_commit_vote(
        self, seq: int, validator: str
    ) -> tuple[bool, TransactionEnvelope | None]:
        async with self.lock:
            self.commit_votes[seq].add(validator)
            if (
                len(self.commit_votes[seq]) >= self.quorum
                and self.local_stage.get(seq) == "PREPARED"
            ):
                self.local_stage[seq] = "COMMITTED"
                self.local_head = seq
                return True, self.pre_prepares.get(seq)
            return False, None

    async def has_pre_prepare(self, seq: int) -> bool:
        async with self.lock:
            return seq in self.pre_prepares
