# -*- coding: utf-8 -*-

from functools import wraps

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed

from .config.graph import StackDependencyGraph


def recurse_sub_stack_groups_with_graph(func):
    return recurse_into_sub_stack_groups(func, StackDependencyGraph)


def recurse_into_sub_stack_groups(func, factory=dict):
    """
    Two types of StackGroups exist, non-leaf and leaf. Non-leaf
    stack_groups contain sub-stack_groups, while leaf
    stack_groups contain stacks. If a command is executed by a leaf
    stack_group, it should execute that command on the stacks it
    contains. If a command is executed by a non-leaf stack_group, it
    should invoke that command on each of its sub-stack_groups. Recurse
    is a decorator used by sceptre.stack_group.StackGroup to do
    this. The function passed, ``func``, must return a dictionary.
    """
    @wraps(func)
    def decorated(self, *args, **kwargs):
        function_name = func.__name__
        responses = factory()
        num_stack_groups = len(self.sub_stack_groups)
        # As commands carried out by sub-stack_groups may be blocking,
        # execute them on separate threads.
        if num_stack_groups:
            with ThreadPoolExecutor(max_workers=num_stack_groups)\
                    as thread_stack_group:
                futures = [
                    thread_stack_group.submit(
                        getattr(stack_group, function_name),
                        *args,
                        **kwargs
                    )
                    for stack_group in self.sub_stack_groups
                ]
                for future in as_completed(futures):
                    response = future.result()
                    if response:
                        responses.update(response)

        response = func(self, *args, **kwargs)
        if response:
            responses.update(response)
        return responses

    return decorated


def get_name_tuple(name):
    """
    Returns a tuple of the stack name, split on the slashes.

    :param name: The Stack's name.
    :type name: str
    :returns: A tuple of the stack's name.
    :rtype: tuple
    """
    return tuple(name.split("/"))


def get_external_stack_name(project_code, stack_name):
    """
    Returns the name given to a stack in CloudFormation.

    :param project_code: The project code, as defined in config.yaml.
    :type project_code: str
    :param stack_name: The name of the stack.
    :type stack_name: str
    :returns: The name given to the stack in CloudFormation.
    :rtype: str
    """
    return "-".join([
        project_code,
        stack_name.replace("/", "-")
    ])


def mask_key(key):
    """
    Returns an masked version of ``key``.

    Returned version has all but the last four characters are replaced with the
    character "*".

    :param key: The string to mask.
    :type key: str
    :returns: An masked version of the key
    :rtype: str
    """
    num_mask_chars = len(key) - 4

    return "".join([
        "*" if i < num_mask_chars else c
        for i, c in enumerate(key)
    ])


def _call_func_on_values(func, attr, cls):
    """
    Searches through dictionary or list for objects of type `cls` and calls the
    supplied function `func`. Supports nested dictionaries and lists.
    Does not detect objects used as keys in dictionaries.

    :param attr: A dictionary or list to search through.
    :type attr: dict or list
    :return: The dictionary or list structure.
    :rtype: dict or list
    """

    def func_on_instance(key):
        if isinstance(value, cls):
            func(attr, key, value)
        elif isinstance(value, list) or isinstance(value, dict):
            _call_func_on_values(func, value, cls)

    if isinstance(attr, dict):
        for key, value in attr.items():
            func_on_instance(key)
    elif isinstance(attr, list):
        for index, value in enumerate(attr):
            func_on_instance(index)
    return attr
