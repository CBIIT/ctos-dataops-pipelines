from unittest.mock import Mock, patch

from opensearch_backup import createSnapshot, selectedIndices


def backup_arguments(indices):
    return {
        "oshost": "https://example.us-east-1.es.amazonaws.com/",
        "repo": "ins",
        "snapshot": "ins-2026-08-24",
        "indices": indices,
    }


def test_selected_indices_are_normalized():
    assert selectedIndices(" home_stats,datasets, files ") == [
        "home_stats",
        "datasets",
        "files",
    ]


def test_backup_uses_only_explicitly_selected_indices():
    response = Mock(status_code=200, text='{"accepted":true}')
    with patch("opensearch_backup.requests.put", return_value=response) as put:
        result = createSnapshot(
            backup_arguments("home_stats,datasets,files,resources"),
            Mock(),
        )

    assert result is response
    assert put.call_args.kwargs["json"]["indices"] == (
        "home_stats,datasets,files,resources"
    )


def test_blank_indices_back_up_all_non_hidden_indices():
    response = Mock(status_code=200, text='{"accepted":true}')
    with patch("opensearch_backup.requests.put", return_value=response) as put:
        createSnapshot(backup_arguments(""), Mock())

    assert put.call_args.kwargs["json"]["indices"] == "*,-.*"
