import os
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from sceptre.cli.helpers import (
    has_wildcard,
    expand_wildcard_to_command_path,
    print_wildcard_matched_stacks,
)
from sceptre.cli.launch import launch_command
from sceptre.cli.create import create_command
from sceptre.cli.update import update_command
from sceptre.cli.delete import delete_command


class TestWildcardHelpers:
    """Tests for wildcard helper functions."""

    def test_has_wildcard_with_asterisk(self):
        """Test has_wildcard detects asterisk."""
        assert has_wildcard("dev/*.yaml") is True

    def test_has_wildcard_with_question_mark(self):
        """Test has_wildcard detects question mark."""
        assert has_wildcard("dev/vpc?.yaml") is True

    def test_has_wildcard_with_double_asterisk(self):
        """Test has_wildcard detects recursive pattern."""
        assert has_wildcard("**/vpc.yaml") is True

    def test_has_wildcard_without_wildcards(self):
        """Test has_wildcard returns False for normal paths."""
        assert has_wildcard("dev/vpc.yaml") is False

    def test_has_wildcard_with_empty_string(self):
        """Test has_wildcard handles empty string."""
        assert has_wildcard("") is False

    @patch("sceptre.cli.helpers.os.path.isfile")
    @patch("sceptre.cli.helpers.glob.glob")
    def test_expand_wildcard_single_match(self, mock_glob, mock_isfile):
        """Test wildcard expansion with single matching file."""
        project_path = "/project"
        config_path = "config"
        pattern = "dev/vpc.yaml"

        # Mock glob to return one match
        mock_glob.return_value = ["/project/config/dev/vpc.yaml"]
        # Mock isfile to return True for all paths
        mock_isfile.return_value = True

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        assert command_path == "dev"
        assert matched_files == ["dev/vpc.yaml"]
        mock_glob.assert_called_once_with(
            "/project/config/dev/vpc.yaml", recursive=True
        )

    @patch("sceptre.cli.helpers.os.path.isfile")
    @patch("sceptre.cli.helpers.glob.glob")
    def test_expand_wildcard_multiple_matches_same_directory(self, mock_glob, mock_isfile):
        """Test wildcard expansion with multiple matches in same directory."""
        project_path = "/project"
        config_path = "config"
        pattern = "dev/*.yaml"

        # Mock glob to return multiple matches in same directory
        mock_glob.return_value = [
            "/project/config/dev/vpc.yaml",
            "/project/config/dev/app.yaml",
            "/project/config/dev/db.yaml",
        ]
        mock_isfile.return_value = True

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        assert command_path == "dev"
        assert set(matched_files) == {"dev/vpc.yaml", "dev/app.yaml", "dev/db.yaml"}

    @patch("sceptre.cli.helpers.os.path.isfile")
    @patch("sceptre.cli.helpers.glob.glob")
    def test_expand_wildcard_multiple_matches_different_directories(self, mock_glob, mock_isfile):
        """Test wildcard expansion with matches across directories."""
        project_path = "/project"
        config_path = "config"
        pattern = "**/vpc.yaml"

        # Mock glob to return matches in different directories
        mock_glob.return_value = [
            "/project/config/dev/vpc.yaml",
            "/project/config/prod/vpc.yaml",
        ]
        mock_isfile.return_value = True

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        # Common parent should be config root
        assert command_path == "."
        assert set(matched_files) == {"dev/vpc.yaml", "prod/vpc.yaml"}

    @patch("sceptre.cli.helpers.os.path.isfile")
    @patch("sceptre.cli.helpers.glob.glob")
    def test_expand_wildcard_filters_config_files(self, mock_glob, mock_isfile):
        """Test that config.yaml files are filtered out."""
        project_path = "/project"
        config_path = "config"
        pattern = "dev/*.yaml"

        # Mock glob to return both stack files and config files
        mock_glob.return_value = [
            "/project/config/dev/config.yaml",
            "/project/config/dev/vpc.yaml",
            "/project/config/dev/config.prod.yaml",
            "/project/config/dev/app.yaml",
        ]
        mock_isfile.return_value = True

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        assert command_path == "dev"
        # Only non-config files should be returned
        assert set(matched_files) == {"dev/vpc.yaml", "dev/app.yaml"}

    @patch("sceptre.cli.helpers.glob.glob")
    def test_expand_wildcard_no_matches(self, mock_glob):
        """Test wildcard expansion with no matches."""
        project_path = "/project"
        config_path = "config"
        pattern = "dev/nonexistent*.yaml"

        mock_glob.return_value = []

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        assert command_path is None
        assert matched_files == []

    @patch("sceptre.cli.helpers.glob.glob")
    def test_expand_wildcard_only_config_files_matched(self, mock_glob):
        """Test when only config.yaml files match (should return None)."""
        project_path = "/project"
        config_path = "config"
        pattern = "dev/config*.yaml"

        mock_glob.return_value = [
            "/project/config/dev/config.yaml",
            "/project/config/dev/config.prod.yaml",
        ]

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        assert command_path is None
        assert matched_files == []

    @patch("sceptre.cli.helpers.os.path.isfile")
    @patch("sceptre.cli.helpers.glob.glob")
    def test_expand_wildcard_with_nested_directories(self, mock_glob, mock_isfile):
        """Test wildcard expansion with nested directory structure."""
        project_path = "/project"
        config_path = "config"
        pattern = "dev/*/*.yaml"

        mock_glob.return_value = [
            "/project/config/dev/us-east-1/vpc.yaml",
            "/project/config/dev/us-west-2/vpc.yaml",
        ]
        mock_isfile.return_value = True

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        assert command_path == "dev"
        assert set(matched_files) == {
            "dev/us-east-1/vpc.yaml",
            "dev/us-west-2/vpc.yaml",
        }

    @patch("sceptre.cli.helpers.glob.glob")
    @patch("sceptre.cli.helpers.os.path.isfile")
    def test_expand_wildcard_filters_directories(self, mock_isfile, mock_glob):
        """Test that directories are filtered out."""
        project_path = "/project"
        config_path = "config"
        pattern = "dev/*"

        # Mock glob to return both files and directories
        mock_glob.return_value = [
            "/project/config/dev/vpc.yaml",
            "/project/config/dev/subdirectory",
        ]
        
        # Only the first one is a file
        mock_isfile.side_effect = lambda x: x.endswith(".yaml")

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        assert command_path == "dev"
        assert matched_files == ["dev/vpc.yaml"]

    @patch("sceptre.cli.helpers.click.echo")
    def test_print_wildcard_matched_stacks(self, mock_echo):
        """Test printing matched stacks."""
        matched_files = ["dev/vpc.yaml", "dev/app.yaml", "prod/vpc.yaml"]
        pattern = "**/vpc.yaml"

        print_wildcard_matched_stacks(matched_files, pattern)

        mock_echo.assert_called_once()
        call_args = mock_echo.call_args[0][0]
        
        # Check that the message contains the pattern
        assert pattern in call_args
        # Check that all matched files are in the message
        for file_path in matched_files:
            assert file_path in call_args

    @patch("sceptre.cli.helpers.click.echo")
    def test_print_wildcard_matched_stacks_empty(self, mock_echo):
        """Test printing with empty list does nothing."""
        print_wildcard_matched_stacks([], "pattern")
        
        # Should not echo anything
        mock_echo.assert_not_called()

    @patch("sceptre.cli.helpers.os.path.isfile")
    @patch("sceptre.cli.helpers.glob.glob")
    def test_expand_wildcard_pattern_with_prefix(self, mock_glob, mock_isfile):
        """Test pattern like foo/*bar.yaml matching foo/testbar.yaml."""
        project_path = "/project"
        config_path = "config"
        pattern = "foo/*bar.yaml"

        mock_glob.return_value = [
            "/project/config/foo/testbar.yaml",
            "/project/config/foo/anothertestbar.yaml",
        ]
        mock_isfile.return_value = True

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        assert command_path == "foo"
        assert set(matched_files) == {"foo/testbar.yaml", "foo/anothertestbar.yaml"}

    @patch("sceptre.cli.helpers.os.path.isfile")
    @patch("sceptre.cli.helpers.glob.glob")
    def test_expand_wildcard_pattern_with_suffix(self, mock_glob, mock_isfile):
        """Test pattern like foo/bar*.yaml matching foo/bar2.yaml and foo/bartest.yaml."""
        project_path = "/project"
        config_path = "config"
        pattern = "foo/bar*.yaml"

        mock_glob.return_value = [
            "/project/config/foo/bar2.yaml",
            "/project/config/foo/bartest.yaml",
        ]
        mock_isfile.return_value = True

        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, pattern
        )

        assert command_path == "foo"
        assert set(matched_files) == {"foo/bar2.yaml", "foo/bartest.yaml"}


class TestCLICommandsWithWildcards:
    """Tests for CLI commands with wildcard support."""

    def setup_method(self, test_method):
        """Set up test fixtures."""
        self.runner = CliRunner()
        self.patcher_plan = patch("sceptre.cli.launch.SceptrePlan")
        self.patcher_launcher = patch("sceptre.cli.launch.Launcher")
        self.mock_plan = self.patcher_plan.start()
        self.mock_launcher = self.patcher_launcher.start()

    def teardown_method(self, test_method):
        """Clean up patches."""
        self.patcher_plan.stop()
        self.patcher_launcher.stop()

    @patch("sceptre.cli.launch.has_wildcard")
    @patch("sceptre.cli.launch.expand_wildcard_to_command_path")
    @patch("sceptre.cli.launch.print_wildcard_matched_stacks")
    def test_launch_with_wildcard_success(
        self, mock_print, mock_expand, mock_has_wildcard
    ):
        """Test launch command with wildcard pattern."""
        mock_has_wildcard.return_value = True
        mock_expand.return_value = (
            "dev",
            ["dev/vpc.yaml", "dev/app.yaml"],
        )
        
        # Mock launcher
        launcher_instance = MagicMock()
        launcher_instance.launch.return_value = 0
        self.mock_launcher.return_value = launcher_instance

        result = self.runner.invoke(
            launch_command,
            ["dev/*.yaml"],
            obj={"project_path": "/project"},
        )

        assert result.exit_code == 0
        mock_has_wildcard.assert_called_once_with("dev/*.yaml")
        mock_expand.assert_called_once()
        mock_print.assert_called_once_with(
            ["dev/vpc.yaml", "dev/app.yaml"], "dev/*.yaml"
        )

    @patch("sceptre.cli.launch.has_wildcard")
    @patch("sceptre.cli.launch.expand_wildcard_to_command_path")
    def test_launch_with_wildcard_no_matches(self, mock_expand, mock_has_wildcard):
        """Test launch command with wildcard that matches nothing."""
        mock_has_wildcard.return_value = True
        mock_expand.return_value = (None, [])

        result = self.runner.invoke(
            launch_command,
            ["dev/nonexistent*.yaml"],
            obj={"project_path": "/project"},
        )

        assert result.exit_code == 1
        assert "No stacks matched pattern" in result.output

    @patch("sceptre.cli.create.has_wildcard")
    @patch("sceptre.cli.create.expand_wildcard_to_command_path")
    @patch("sceptre.cli.create.print_wildcard_matched_stacks")
    @patch("sceptre.cli.create.SceptrePlan")
    def test_create_with_wildcard_and_changeset(
        self, mock_plan_class, mock_print, mock_expand, mock_has_wildcard
    ):
        """Test create command with wildcard and change-set name."""
        mock_has_wildcard.return_value = True
        mock_expand.return_value = ("dev", ["dev/vpc.yaml", "dev/app.yaml"])
        
        mock_plan = MagicMock()
        mock_plan_class.return_value = mock_plan

        result = self.runner.invoke(
            create_command,
            ["dev/*.yaml", "my-change-set", "-y"],
            obj={"project_path": "/project"},
        )

        assert result.exit_code == 0
        mock_print.assert_called_once()
        # Verify that create_change_set was called on the plan
        mock_plan.create_change_set.assert_called_once_with("my-change-set")

    @patch("sceptre.cli.delete.has_wildcard")
    @patch("sceptre.cli.delete.expand_wildcard_to_command_path")
    @patch("sceptre.cli.delete.print_wildcard_matched_stacks")
    @patch("sceptre.cli.delete.SceptreContext")
    @patch("sceptre.cli.delete.SceptrePlan")
    @patch("sceptre.cli.delete.confirmation")
    @patch("sceptre.cli.delete.stack_status_exit_code")
    @patch("builtins.print")
    def test_delete_with_wildcard_forces_confirmation(
        self, mock_print_fn, mock_exit_code, mock_confirmation, 
        mock_plan_class, mock_context_class, mock_print_stacks, 
        mock_expand, mock_has_wildcard
    ):
        """Test delete command with wildcard forces confirmation even with -y flag."""
        # Set up wildcard detection and expansion
        mock_has_wildcard.return_value = True
        mock_expand.return_value = ("dev", ["dev/vpc.yaml", "dev/app.yaml"])
        
        # Mock SceptreContext creation
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        
        # Mock SceptrePlan creation and methods
        mock_plan = MagicMock()
        mock_plan_class.return_value = mock_plan
        
        # Create a mock stack for iteration
        mock_stack = MagicMock()
        mock_stack.name = "dev/vpc"
        
        # Mock plan iteration (used in delete command to print stack list)
        mock_plan.__iter__.return_value = iter([mock_stack])
        
        # Mock plan.delete to return a dict of responses
        mock_plan.delete.return_value = {mock_stack: "DELETE_COMPLETE"}
        mock_exit_code.return_value = 0
        
        # Mock plan.delete.__name__ attribute for confirmation message
        mock_plan.delete.__name__ = "delete"

        result = self.runner.invoke(
            delete_command,
            ["dev/*.yaml", "-y"],
            obj={
                "project_path": "/project",
                "user_variables": {},
                "options": {},
                "ignore_dependencies": False,
            },
        )

        # If the command failed, print debugging info
        if result.exit_code != 0 or not mock_confirmation.called:
            print(f"\nExit code: {result.exit_code}")
            print(f"Output: {result.output}")
            if result.exception:
                print(f"Exception: {result.exception}")
                import traceback
                traceback.print_exception(
                    type(result.exception), 
                    result.exception, 
                    result.exception.__traceback__
                )

        # Verify confirmation was called
        assert mock_confirmation.called, \
            f"confirmation was not called. Exit code: {result.exit_code}"
        
        # Verify that yes=False despite the -y flag
        call_args = mock_confirmation.call_args
        assert call_args[0][1] is False, \
            f"Expected yes=False, got yes={call_args[0][1]}"

    @patch("sceptre.cli.update.has_wildcard")
    @patch("sceptre.cli.update.expand_wildcard_to_command_path")
    @patch("sceptre.cli.update.print_wildcard_matched_stacks")
    @patch("sceptre.cli.update.SceptrePlan")
    def test_update_with_wildcard(
        self, mock_plan_class, mock_print, mock_expand, mock_has_wildcard
    ):
        """Test update command with wildcard pattern."""
        mock_has_wildcard.return_value = True
        mock_expand.return_value = ("prod", ["prod/vpc.yaml", "prod/app.yaml"])
        
        mock_plan = MagicMock()
        mock_plan.update.return_value = {}
        mock_plan_class.return_value = mock_plan

        result = self.runner.invoke(
            update_command,
            ["prod/*.yaml", "-y"],
            obj={"project_path": "/project"},
        )

        assert result.exit_code == 0
        mock_print.assert_called_once_with(
            ["prod/vpc.yaml", "prod/app.yaml"], "prod/*.yaml"
        )
        mock_plan.update.assert_called_once()

    @patch("sceptre.cli.launch.has_wildcard")
    def test_launch_without_wildcard(self, mock_has_wildcard):
        """Test launch command with normal path (no wildcard)."""
        mock_has_wildcard.return_value = False
        
        launcher_instance = MagicMock()
        launcher_instance.launch.return_value = 0
        self.mock_launcher.return_value = launcher_instance

        result = self.runner.invoke(
            launch_command,
            ["dev/vpc.yaml"],
            obj={"project_path": "/project"},
        )

        # Should not call expand functions
        assert result.exit_code == 0
        mock_has_wildcard.assert_called_once_with("dev/vpc.yaml")
