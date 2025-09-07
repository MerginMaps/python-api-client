import logging
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("status_code", [502, 504])
def test_push_finalize_logs_on_5xx_real_diff(caplog, status_code):
    # geodiff is required – if missing, skip the test gracefully
    pytest.importorskip("pygeodiff")
    from pygeodiff import GeoDiff

    from mergin.client_push import push_project_finalize
    from mergin.common import ClientError

    # --- exact paths according to your repo structure ---
    data_dir = Path(__file__).resolve().parent / "test_data"
    base = data_dir / "base.gpkg"
    modified = data_dir / "inserted_1_A.gpkg"

    # if something is missing, print the directory contents and FAIL
    if not base.exists() or not modified.exists():
        print("[DEBUG] data_dir:", data_dir)
        print("[DEBUG] data_dir.exists():", data_dir.exists())
        if data_dir.exists():
            print("[DEBUG] contents:", [p.name for p in sorted(data_dir.iterdir())])
        pytest.fail(f"Expected files are not available: base={base.exists()} inserted_1_A={modified.exists()}")

    # --- create a real .diff (changeset) ---
    with tempfile.TemporaryDirectory(prefix="geodiff-") as tmpdir:
        tmpdir = Path(tmpdir)
        diff_path = tmpdir / "base_to_inserted_1_A.diff"

        gd = GeoDiff()
        gd.create_changeset(str(base), str(modified), str(diff_path))
        diff_size = diff_path.stat().st_size
        file_size = modified.stat().st_size

        # --- logger captured by caplog ---
        logger = logging.getLogger("mergin_test")
        logger.setLevel(logging.DEBUG)
        logger.propagate = True

        # --- fake mc.post: /finish -> 5xx, /cancel -> ok ---
        def fake_post(url, *args, **kwargs):
            if "/finish/" in url:
                err = ClientError("Gateway error")
                setattr(err, "http_error", status_code)
                raise err
            if "/cancel/" in url:
                return SimpleNamespace(msg="cancelled")
            return SimpleNamespace(msg="ok")

        # --- fake future/executor (no exceptions) ---
        fake_future = SimpleNamespace(done=lambda: True, exception=lambda: None, running=lambda: False)
        fake_executor = SimpleNamespace(shutdown=lambda wait=True: None)

        # mp.fpath_meta should resolve the relative diff path to an absolute path
        def fpath_meta(rel):
            return str(diff_path) if rel == diff_path.name else rel

        # --- minimal job – only what finalize() uses ---
        job = SimpleNamespace(
            executor=fake_executor,
            futures=[fake_future],
            transferred_size=1234,
            total_size=1234,
            upload_queue_items=[1],
            transaction_id="tx-1",
            mp=SimpleNamespace(
                log=logger,
                update_metadata=lambda *a, **k: None,
                apply_push_changes=lambda *a, **k: None,
                fpath_meta=fpath_meta,
            ),
            mc=SimpleNamespace(post=fake_post),
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
            tmp_dir=SimpleNamespace(cleanup=lambda: None),
        )

        # capture ERROR-level logs from "mergin_test"
        caplog.set_level(logging.ERROR, logger="mergin_test")

        # --- call production logic (expecting 5xx) ---
        with pytest.raises(ClientError):
            push_project_finalize(job)

        text = caplog.text

        assert f"Push failed with HTTP error {status_code}" in text
        assert "Upload details:" in text
        assert "Files:" in text
        assert modified.name in text
        assert f"size={file_size}" in text
        assert f"diff_size={diff_size}" in text
        # helper may compute changes, or log changes=n/a – accept both
        assert ("changes=" in text) or ("changes=n/a" in text)
