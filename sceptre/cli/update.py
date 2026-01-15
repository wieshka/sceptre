from uuid import uuid1

import click

from typing import Optional
from sceptre.context import SceptreContext
from sceptre.cli.helpers import (
    catch_exceptions,
    confirmation,
    write,
    stack_status_exit_code,
    simplify_change_set_description,
    has_wildcard,
    expand_wildcard_to_command_path,
    print_wildcard_matched_stacks,
)
from sceptre.stack_status import StackChangeSetStatus
from sceptre.plan.plan import SceptrePlan


@click.command(name="update", short_help="Update a stack.")
@click.argument("path")
@click.option(
    "-c", "--change-set", is_flag=True, help="Create a change set before updating."
)
@click.option("-v", "--verbose", is_flag=True, help="Display verbose output.")
@click.option("-y", "--yes", is_flag=True, help="Assume yes to all questions.")
@click.option(
    "--disable-rollback/--enable-rollback",
    default=None,
    help="Disable or enable the cloudformation automatic rollback",
)
@click.pass_context
@catch_exceptions
def update_command(
    ctx, path, change_set, verbose, yes, disable_rollback: Optional[bool]
):
    """
    Updates a stack for a given config PATH. Or perform an update via
    change-set when the change-set flag is set.
    
    Supports wildcard patterns (e.g., 'dev/*.yaml', '**/vpc.yaml') to match multiple stacks.
    \f

    :param path: Path to execute the command on.
    :type path: str
    :param change_set: Whether a change set should be created.
    :type change_set: bool
    :param verbose: A flag to print a verbose output.
    :type verbose: bool
    :param yes: A flag to answer 'yes' to all CLI questions.
    :type yes: bool
    """
    # Handle wildcard expansion
    if has_wildcard(path):
        project_path = ctx.obj.get("project_path")
        config_path = "config"  # Default config path
        command_path, matched_files = expand_wildcard_to_command_path(
            project_path, config_path, path
        )

        if command_path is None:
            click.echo(f"No stacks matched pattern '{path}'")
            exit(1)

        print_wildcard_matched_stacks(matched_files, path)
        path = command_path

    context = SceptreContext(
        command_path=path,
        command_params=ctx.params,
        project_path=ctx.obj.get("project_path"),
        user_variables=ctx.obj.get("user_variables"),
        options=ctx.obj.get("options"),
        output_format=ctx.obj.get("output_format"),
        ignore_dependencies=ctx.obj.get("ignore_dependencies"),
    )

    plan = SceptrePlan(context)

    if change_set:
        change_set_name = "-".join(["change-set", uuid1().hex])
        plan.create_change_set(change_set_name)
        try:
            # Wait for change set to be created
            statuses = plan.wait_for_cs_completion(change_set_name)
            # Exit if change set fails to create
            for status in list(statuses.values()):
                if status != StackChangeSetStatus.READY:
                    exit(1)

            # Describe changes
            descriptions = plan.describe_change_set(change_set_name)
            for description in list(descriptions.values()):
                if not verbose:
                    description = simplify_change_set_description(description)
                write(description, context.output_format)

            # Execute change set if happy with changes
            if yes or click.confirm("Proceed with stack update?"):
                plan.execute_change_set(change_set_name)
        except Exception as e:
            raise e
        finally:
            # Clean up by deleting change set
            plan.delete_change_set(change_set_name)
    else:
        confirmation("update", yes, command_path=path)
        responses = plan.update()
        exit(stack_status_exit_code(responses.values()))
