from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from sceptre.cli import path_processor
from sceptre.cli.delete import delete_command
from sceptre.cli.path_processor import (
    PathProcessor,
    get_registered_processors,
    process_path,
)


class WildcardProcessor(PathProcessor):
    def can_process(self, path):
        return "*" in path

    def process(self, path, project_path, search_dir, command):
        return "dev", True, ["dev/a.yaml", "dev/b.yaml"]


class NeverProcessor(PathProcessor):
    def can_process(self, path):
        return False


class BrokenProcessor(PathProcessor):
    def can_process(self, path):
        raise RuntimeError("boom")


class NotAProcessor:
    pass


def make_entry_point(name, returned_class=None, load_error=None):
    entry_point = MagicMock()
    entry_point.name = name
    if load_error:
        entry_point.load.side_effect = load_error
    else:
        entry_point.load.return_value = lambda: returned_class
    return entry_point


def patch_entry_points(*eps):
    mock_eps = MagicMock()
    mock_eps.select.return_value = list(eps)
    return patch.object(path_processor, "entry_points", return_value=mock_eps)


class TestGetRegisteredProcessors:
    def test_returns_empty_when_no_plugins(self):
        with patch_entry_points():
            assert get_registered_processors() == {}

    def test_registers_valid_processor(self):
        with patch_entry_points(make_entry_point("wildcard", WildcardProcessor)):
            assert get_registered_processors() == {"wildcard": WildcardProcessor}

    def test_skips_non_path_processor(self):
        with patch_entry_points(make_entry_point("bad", NotAProcessor)):
            assert get_registered_processors() == {}

    def test_skips_plugin_that_fails_to_load(self):
        with patch_entry_points(
            make_entry_point("broken", load_error=ImportError("nope")),
            make_entry_point("wildcard", WildcardProcessor),
        ):
            assert get_registered_processors() == {"wildcard": WildcardProcessor}


class TestProcessPath:
    def test_returns_path_unchanged_without_plugins(self):
        with patch_entry_points():
            assert process_path("dev/vpc.yaml", "/project") == (
                "dev/vpc.yaml",
                False,
                None,
            )

    def test_returns_path_unchanged_when_no_processor_matches(self):
        with patch_entry_points(make_entry_point("never", NeverProcessor)):
            assert process_path("dev/vpc.yaml", "/project") == (
                "dev/vpc.yaml",
                False,
                None,
            )

    def test_delegates_to_matching_processor(self):
        with patch_entry_points(make_entry_point("wildcard", WildcardProcessor)):
            assert process_path("dev/*.yaml", "/project", "config", "launch") == (
                "dev",
                True,
                ["dev/a.yaml", "dev/b.yaml"],
            )

    def test_skips_processor_that_raises(self):
        with patch_entry_points(
            make_entry_point("broken", BrokenProcessor),
            make_entry_point("wildcard", WildcardProcessor),
        ):
            path, force_confirm, _ = process_path("dev/*.yaml", "/project")
        assert path == "dev"
        assert force_confirm is True


class TestCliIntegration:
    def test_force_confirm_overrides_yes_flag(self):
        ctx_obj = {
            "project_path": "/project",
            "user_variables": {},
            "options": {},
            "ignore_dependencies": False,
        }
        with patch(
            "sceptre.cli.delete.process_path", return_value=("dev", True, None)
        ) as mock_process_path, patch("sceptre.cli.delete.SceptreContext"), patch(
            "sceptre.cli.delete.SceptrePlan"
        ) as mock_plan, patch(
            "sceptre.cli.delete.confirmation"
        ) as mock_confirmation:
            mock_plan.return_value.delete.__name__ = "delete"
            CliRunner().invoke(delete_command, ["dev/*.yaml", "--yes"], obj=ctx_obj)

        mock_process_path.assert_called_once_with(
            "dev/*.yaml", "/project", "config", "delete"
        )
        assert mock_confirmation.call_args.args[1] is False
