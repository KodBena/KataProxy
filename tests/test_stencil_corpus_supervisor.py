from __future__ import annotations

from pathlib import Path

import pytest

from transformers.stencil_corpus_supervisor import (
    StencilCorpusMaterializerSupervisor,
)


@pytest.mark.asyncio
async def test_supervisor_reports_database_startup_failure(tmp_path: Path) -> None:
    obstruction = tmp_path / "not-a-directory"
    obstruction.write_text("occupied", encoding="utf-8")
    supervisor = StencilCorpusMaterializerSupervisor(
        obstruction / "corpus.sqlite3",
        startup_timeout_seconds=3.0,
    )

    with pytest.raises(RuntimeError, match="failed before startup readiness"):
        await supervisor.start()

    assert not supervisor.is_running
    assert supervisor.worker_pid is None
