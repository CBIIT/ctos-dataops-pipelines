from unittest.mock import Mock, patch

import pytest

from opensearch_restore import (
    checkSnapshotExists,
    deleteIndexes,
    listSnapshots,
    opensearch_restore,
    restoreIndexes,
    selectedIndices,
)


def restore_arguments(indices):
    return {
        "oshost": "https://example.us-east-1.es.amazonaws.com/",
        "repo": "ins",
        "snapshot": "ins-2026-08-21-all",
        "indices": indices,
    }


def test_selected_indices_are_normalized():
    assert selectedIndices(" programs,projects, files ") == [
        "programs",
        "projects",
        "files",
    ]


def test_snapshot_preflight_accepts_existing_snapshot():
    arguments = restore_arguments([])
    response = Mock(
        ok=True,
        status_code=200,
        text='{"snapshots":[{"snapshot":"ins-2026-08-21-all"}]}',
    )
    response.json.return_value = {
        "snapshots": [{"snapshot": "ins-2026-08-21-all"}]
    }

    with patch("opensearch_restore.requests.get", return_value=response) as get:
        checkSnapshotExists(arguments, Mock())

    assert get.call_args.args[0] == (
        arguments["oshost"] + "_snapshot/ins/ins-2026-08-21-all"
    )


def test_available_snapshots_are_listed(capsys):
    arguments = restore_arguments([])
    response = Mock(ok=True, status_code=200)
    response.json.return_value = {
        "snapshots": [
            {"snapshot": "ins-2026-08-20-all"},
            {"snapshot": "ins-2026-08-21-all"},
        ]
    }

    with patch("opensearch_restore.requests.get", return_value=response) as get:
        snapshot_names = listSnapshots(arguments, Mock())

    assert snapshot_names == ["ins-2026-08-20-all", "ins-2026-08-21-all"]
    assert "available snapshots: ['ins-2026-08-20-all', 'ins-2026-08-21-all']" in (
        capsys.readouterr().out
    )
    assert get.call_args.args[0] == arguments["oshost"] + "_snapshot/ins/_all"


def test_missing_snapshot_stops_restore_before_indices_are_deleted():
    arguments = restore_arguments([])
    response = Mock(
        ok=False,
        status_code=404,
        text='{"error":"snapshot_missing_exception"}',
    )

    with patch("opensearch_restore.osAuth"), patch(
        "opensearch_restore.registerRepo"
    ), patch("opensearch_restore.requests.get", return_value=response), patch(
        "opensearch_restore.deleteIndexes"
    ) as delete, patch("opensearch_restore.restoreIndexes") as restore:
        with pytest.raises(Exception, match="No indices were deleted"):
            opensearch_restore(arguments)

    delete.assert_not_called()
    restore.assert_not_called()


def test_restore_uses_only_explicitly_selected_indices():
    response = Mock(status_code=200, text='{"accepted":true}')
    with patch("opensearch_restore.requests.post", return_value=response) as post:
        result = restoreIndexes(
            restore_arguments(["programs", "projects", "publications"]),
            Mock(),
        )

    assert result is response
    assert post.call_args.kwargs["json"]["indices"] == (
        "programs,projects,publications"
    )


def test_blank_indices_restore_all_non_hidden_indices():
    response = Mock(status_code=200, text='{"accepted":true}')
    with patch("opensearch_restore.requests.post", return_value=response) as post:
        restoreIndexes(restore_arguments([]), Mock())

    assert post.call_args.kwargs["json"]["indices"] == "*,-.*"


def test_delete_uses_the_same_normalized_selection_as_restore():
    exists = Mock(status_code=200)
    deleted = Mock(text='{"acknowledged":true}')
    arguments = restore_arguments(" programs, projects ")

    with patch("opensearch_restore.requests.get", return_value=exists), patch(
        "opensearch_restore.requests.delete",
        return_value=deleted,
    ) as delete, patch("opensearch_restore.time.sleep"):
        deleteIndexes(arguments, Mock())

    assert [call.args[0] for call in delete.call_args_list] == [
        arguments["oshost"] + "programs",
        arguments["oshost"] + "projects",
    ]
