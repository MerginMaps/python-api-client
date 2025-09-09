from types import SimpleNamespace
from pathlib import Path
import logging
import tempfile
import pytest

from pygeodiff import GeoDiff
from mergin.client_push import push_project_finalize, UploadJob
from mergin.common import ClientError

@pytest.mark.parametrize("status_code", [502, 504])
def test_push_finalize_logs_on_5xx_real_diff(caplog, status_code, tmp_path):
    # --- data ---
    data_dir = Path(__file__).resolve().parent / "test_data"
    base = data_dir / "base.gpkg"
    modified = data_dir / "inserted_1_A.gpkg"
    assert base.exists()
    assert modified.exists()

    diff_path = tmp_path / "base_to_inserted_1_A.diff"
    GeoDiff().create_changeset(str(base), str(modified), str(diff_path))
    diff_size = diff_path.stat().st_size
    file_size = modified.stat().st_size

    # --- logger ---
    logger = logging.getLogger("mergin_test")
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
    caplog.set_level(logging.ERROR, logger="mergin_test")

    # --- fake MP/MC len tam, kde treba ---
    class MP:
        def __init__(self, log): self.log = log
        def update_metadata(self, *a, **k): pass
        def apply_push_changes(self, *a, **k): pass
        def fpath_meta(self, rel): return str(diff_path) if rel == diff_path.name else rel

    tx = "tx-1"

    class MC:
        def post(self, url, *args, **kwargs):
            if url.endswith(f"/v1/project/push/finish/{tx}"):
                err = ClientError("Gateway error"); setattr(err, "http_error", status_code); raise err
            if url.endswith(f"/v1/project/push/cancel/{tx}"):
                return SimpleNamespace(msg="cancelled")
            return SimpleNamespace(msg="ok")

    tmp_dir = tempfile.TemporaryDirectory(prefix="python-api-client-test-")

    # --- real UploadJob objekt ---
    job = UploadJob(
        project_path="u/p",
        changes={
            "added": [],
            "updated": [{
                "path": modified.name,
                "size": file_size,
                "diff": {"path": diff_path.name, "size": diff_size},
                "chunks": [1],
            }],
            "removed": [],
        },
        transaction_id=tx,
        mp=MP(logger),
        mc=MC(),
        tmp_dir=tmp_dir,
    )

    # nastav to, čo finalize() očakáva
    job.total_size = 1234
    job.transferred_size = 1234
    job.upload_queue_items = [1]  # len pre log „Upload details“
    job.executor = SimpleNamespace(shutdown=lambda wait=True: None)
    job.futures = [SimpleNamespace(done=lambda: True, exception=lambda: None, running=lambda: False)]
    job.server_resp = {"version": "n/a"}  # aj tak sa 5xx vyhodí skôr

    with pytest.raises(ClientError):
        push_project_finalize(job)

    text = caplog.text
    assert f"Push failed with HTTP error {status_code}" in text
    assert "Upload details:" in text
    assert "Files:" in text
    assert modified.name in text
    assert f"size={file_size}" in text
    assert f"diff_size={diff_size}" in text
    assert ("changes=" in text) or ("changes=n/a" in text)