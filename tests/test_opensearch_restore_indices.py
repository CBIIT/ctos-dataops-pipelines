from unittest.mock import Mock, patch

from opensearch_restore import deleteIndexes, restoreIndexes, selectedIndices


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
