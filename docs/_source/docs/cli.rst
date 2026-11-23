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

Concurrency Control
-------------------

Sceptre supports limiting the number of stacks that are processed concurrently during operations like ``launch``, ``create``, and ``update``. By default, Sceptre will process stacks concurrently based on their dependency relationships, which can result in a large number of concurrent operations for stacks with few dependencies.

To limit concurrency, use the ``--max-concurrency`` option:

.. code-block:: text

   sceptre launch my-stack-group --max-concurrency 5

This will ensure that no more than 5 stacks are processed concurrently, which can be useful when:

* Working with services that have rate limits (e.g., LocalStack during testing)
* Managing resource consumption on the machine running Sceptre
* Avoiding overwhelming downstream services with too many concurrent requests

The ``--max-concurrency`` option is available for the following commands:

* ``launch`` - Limit concurrent stack launches
* ``create`` - Limit concurrent stack creations
* ``update`` - Limit concurrent stack updates

.. note::
   The concurrency limit applies within each dependency batch. Sceptre will still respect stack dependencies and process stacks in the correct order, but will limit the number of concurrent operations within each batch of independent stacks.

Path Processor Plugins
----------------------

The ``create``, ``update``, ``launch`` and ``delete`` commands pass their
``PATH`` argument through any installed *path processor* plugins before
resolving stacks. This lets external packages add features such as
wildcard/glob expansion (for example `sceptre-wildcard-plugin
<https://github.com/wieshka/sceptre-wildcard-plugin>`_) without changes to
Sceptre itself. If no plugin claims the path, it is used unchanged.

A plugin subclasses ``sceptre.cli.path_processor.PathProcessor``:

.. code-block:: python

   from sceptre.cli.path_processor import PathProcessor

   class WildcardProcessor(PathProcessor):
       def can_process(self, path):
           return "*" in path

       def process(self, path, project_path, search_dir, command):
           # Return (processed_path, force_confirm, matched_files)
           ...

   def register_plugin():
       return WildcardProcessor

and registers a function returning that class under the
``sceptre.cli.path_processors`` entry point group:

.. code-block:: toml

   [project.entry-points."sceptre.cli.path_processors"]
   wildcard = "my_package:register_plugin"

``processed_path`` is the stack or StackGroup path the command runs against.
``matched_files`` is an optional list of stack config paths relative to the
``config`` directory (for example ``dev/vpc.yaml``); when given, the command
only acts on those stacks under ``processed_path`` (dependencies are still
resolved as usual). Return ``None`` to act on every stack under
``processed_path``.

The first registered processor whose ``can_process`` returns ``True`` handles
the path. If it returns ``force_confirm=True``, Sceptre asks for confirmation
even when ``--yes`` was passed. Plugins that fail to load or raise errors are
logged as warnings and skipped.

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
