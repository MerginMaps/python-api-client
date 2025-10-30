from types import SimpleNamespace
from pathlib import Path
import logging
import tempfile
from unittest.mock import MagicMock
import pytest

from pygeodiff import GeoDiff
from mergin.client_push import push_project_finalize, UploadJob
from mergin.common import ClientError
from mergin.merginproject import MerginProject
from mergin.client import MerginClient


@pytest.mark.parametrize("status_code", [502, 504])
def test_push_finalize_logs_on_5xx_real_diff(caplog, status_code, tmp_path):
    # test data
    data_dir = Path(__file__).resolve().parent / "test_data"
    base = data_dir / "base.gpkg"
    modified = data_dir / "inserted_1_A.gpkg"
    assert base.exists() and modified.exists()

    # real MerginProject in temp dir
    proj_dir = tmp_path / "proj"
    meta_dir = proj_dir / ".mergin"
    meta_dir.mkdir(parents=True)
    mp = MerginProject(str(proj_dir))

    # route MP logs into pytest caplog
    logger = logging.getLogger("mergin_test")
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
    caplog.set_level(logging.ERROR, logger="mergin_test")
    mp.log = logger

    # generate a real diff into .mergin/
    diff_path = meta_dir / "base_to_inserted_1_A.diff"
    GeoDiff().create_changeset(str(base), str(modified), str(diff_path))
    diff_rel = diff_path.name
    diff_size = diff_path.stat().st_size
    file_size = modified.stat().st_size

    # mock MerginClient: only patch post(); simulate 5xx on finish
    tx = "tx-1"

    def mc_post(url, *args, **kwargs):
        if url.endswith(f"/v1/project/push/finish/{tx}"):
            err = ClientError("Gateway error")
            setattr(err, "http_error", status_code)  # emulate HTTP code on the exception
            raise err
        if url.endswith(f"/v1/project/push/cancel/{tx}"):
            return SimpleNamespace(msg="cancelled")
        return SimpleNamespace(msg="ok")

    mc = MagicMock(spec=MerginClient)
    mc.post.side_effect = mc_post

    tmp_dir = tempfile.TemporaryDirectory(prefix="python-api-client-test-")

    # build a real UploadJob that references the diff/file sizes
    job = UploadJob(
        project_path="u/p",
        changes={
            "added": [],
            "updated": [
                {
                    "path": modified.name,
                    "size": file_size,
                    "diff": {"path": diff_rel, "size": diff_size},
                    "chunks": [1],
                }
            ],
            "removed": [],
        },
        transaction_id=tx,
        mp=mp,
        mc=mc,
        tmp_dir=tmp_dir,
    )

    job.total_size = 1234
    job.transferred_size = 1234
    job.upload_queue_items = [1]
    job.executor = SimpleNamespace(shutdown=lambda wait=True: None)
    job.futures = [SimpleNamespace(done=lambda: True, exception=lambda: None, running=lambda: False)]
    job.server_resp = {"version": "n/a"}

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
