Command Line Interface
======================

Sceptre can be used as a command line tool.
Running Sceptre without a sub-command will display help, showing a list of the
available commands.

Autocomplete
------------

To enable CLI autocomplete for subcommands and parameters execute the
following command:

+----------+-------------------------------------------------+
| shell    | command                                         |
+==========+=================================================+
| bash     | eval "$(_SCEPTRE_COMPLETE=source_bash sceptre)" |
+----------+-------------------------------------------------+
| zsh      | eval "$(_SCEPTRE_COMPLETE=source_zsh sceptre)"  |
+----------+-------------------------------------------------+

Export Stack Outputs to Environment Variables
---------------------------------------------

Stack outputs can be exported as environment variables with the command:

``eval $(sceptre --ignore-dependencies list outputs STACKGROUP/STACK.yaml --export=envvar)``

Note that Sceptre prepends the string ``SCEPTRE_`` to the name of the
environment variable:

.. code-block:: text

   env | grep SCEPTRE
   SCEPTRE_<output_name>=<output_value>

.. _wildcard-support:

Wildcard Support
----------------

Sceptre supports wildcard patterns in command paths for mutating commands such
as ``launch``, ``create``, ``update``, and ``delete``. This allows you to
operate on multiple stacks that match a specific pattern.

Wildcard Patterns
~~~~~~~~~~~~~~~~~

Sceptre supports standard glob-style wildcard patterns:

- ``*`` matches any sequence of characters in a single directory level
- ``?`` matches any single character
- ``**`` matches directories recursively

Examples
~~~~~~~~

Launch all stacks in the dev environment:

.. code-block:: text

   $ sceptre launch dev/*.yaml

Create all VPC stacks across all environments:

.. code-block:: text

   $ sceptre create **/vpc.yaml

Update all stacks whose names start with "app":

.. code-block:: text

   $ sceptre update prod/app*.yaml

Delete all stacks in a specific directory and subdirectories:

.. code-block:: text

   $ sceptre delete dev/**/*.yaml

Pattern Matching Behavior
~~~~~~~~~~~~~~~~~~~~~~~~~~

When using wildcards, Sceptre will:

1. Expand the pattern to match all Stack config files (files ending in ``.yaml``
   or ``.json`` that are not named ``config.yaml`` or ``config.json``)
2. Display a list of matched stacks before executing the command
3. Respect the dependency graph between matched stacks
4. Preserve config inheritance from parent ``config.yaml`` files

For example, given this directory structure:

.. code-block:: text

   config/
   ├── dev/
   │   ├── config.yaml
   │   ├── vpc.yaml
   │   └── app.yaml
   └── prod/
       ├── config.yaml
       ├── vpc.yaml
       └── app.yaml

The pattern ``**/vpc.yaml`` will match both ``dev/vpc.yaml`` and ``prod/vpc.yaml``,
and each will inherit configuration from their respective ``config.yaml`` files.

Safety Features
~~~~~~~~~~~~~~~

Delete commands with wildcards always require explicit confirmation, even when
the ``--yes`` flag is provided. This prevents accidental deletion of multiple
stacks.

.. code-block:: text

   $ sceptre delete --yes dev/*.yaml
   # Will still prompt for confirmation due to wildcard usage

If a wildcard pattern matches no stacks, Sceptre will display an error message
and exit without performing any operations.

Variable Handling
-----------------

You can pass variables into your project using ``--var-file`` and ``--var``.

Variables passed in with ``--var`` will overwrite any matching variables specified in
``--var-file``. If you use multiple ``--var`` flags then the right-most ``--var`` will
overwrite any matching ``--vars`` to the left. For example, in the following command

``sceptre --var var1=one --var var2=two --var var1=three launch stack``

``var1`` will equal ``three``.

You can also use ``--var`` to overwrite nested keys in a ``--var-file``. For example,
given a variable file "vars.yaml":

.. code-block:: yaml

  # vars.yaml
  ---
  top:
    middle:
      nested: hello
    middle2:
      nested: world

we could overwrite ``nested: world`` to ``nested: hi`` using:

``sceptre --var-file vars.yaml --var top.middle2.nested=hi launch stack``

.. note::
  Sceptre will load your entire project to build a full dependency graph.
  This means that all stacks that use variables will need to have a value
  provided to them - even if they are not in your ``command_path`` or are not
  a dependency. Using a --var-file with all variables set can help meet this
  requirement.

It is also possible to have keys merged according to a deep merge
algorithm from successive var files, by specifying ``--merge-vars``. So, if we
had a second variable file "vars2.yaml":

.. code-block:: yaml

  # other_vars.yaml
  ---
  top:
    middle3:
      nested: more world


We could merge all of this together using:

``sceptre --merge-vars --var-file vars.yaml --var-file other_vars.yaml launch stack``

The ``top`` dictionary would then be expected to contain:

.. code-block:: python

  {
    "top": {
      "middle": {"nested": "hello"},
      "middle2": {"nested": "world"},
      "middle3": {"nested": "more world"}
    }
  }

Command reference
-----------------

Command options differ depending on the command, and can be found by running:

.. code-block:: text

   sceptre
   sceptre --help
   sceptre COMMAND --help


.. click:: sceptre.cli:cli
  :prog: sceptre
  :show-nested:
