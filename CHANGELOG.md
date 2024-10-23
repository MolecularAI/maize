# CHANGELOG

## Version 0.8.5
## Features
- Added node tagging system

## Changes
- Removed excessive debug spam for post-run directory cleanup
- Set env variables before module loading
- Changed PSI/J logging to debug

## Fixes
- Fixed subgraphs not providing correct serialized summaries
- Fixed serialized_summary not including parent classes (fixes #43)
- `serialized_summary()` now provides correct type information (fixes #44)

## Version 0.8.4
## Fixes
- Fixed required parameters defined in config not being recognized
- Changed directory setup to use unique IDs instead of incrementing to avoid race conditions

## Version 0.8.3
## Fixes
- Fixed incorrect working directory being used in some cases

## Version 0.8.2
## Features
- Command batching option for `run_multi`

## Changes
- Improved GPU status querying

## Fixes
- Fixed excessive writing of temporary job output files
- Pinned Numpy to <2.0 for now

## Version 0.8.1
### Features
- Workflow submission improvements

### Changes
- Added more detailed job logging

### Fixes
- Fix for excessive queue polling on workflow submission
- Some minor SonarQube fixes

## Version 0.8.0
### Features
- Workflow submission functionality
- Added `wait_for_all` command to wait for multiple submitted workflows
- Added `gpu_info` utility to determine if it's safe to launch a command on GPU

### Changes
- Parameters with a default are now automatically considered optional
- Modified JobHandler to cancel jobs on interrupt (#36)
- Multiple async job submission attempts

### Fixes
- Ensure that iterables of paths are correctly handled
- Various typing fixes

## Version 0.7.10
### Changes
- `parallel` can now run with only constant inputs

### Fixes
- All node superclasses are now searched for type information
- Fix for incorrect generic type updates
- Fixed inconsistencies between `flow.scratch` and `flow.config.scratch`

## Version 0.7.9
### Changes
- Added logger flushing on shutdown
- Slightly improved error message for duplicate nodes
- Added extra batch job logging

## Version 0.7.8
### Features
- Added version flag

### Changes
- Unset workflow-level parameters no longer raise (fixes #22)
- Writing to yaml now encodes paths as strings

### Fixes
- Fixed sonarqube bugs
- Fixed off-by-one in job completion logging

## Version 0.7.7
### Features
- Added active flag to all nodes to enable instant shutdown

### Changes
- Improved graph connection logging

## Version 0.7.6
### Features
- Added `MultiParameter` hook functionality to allow complex parameter mappings

### Changes
- As a result of the hook functionality to `MultiParameter`, this class is now a double generic. If you use it in your code directly (unlikely), you will need to update the type signature.

### Fixes
- Fixed wrong order in shell variable expansion for config command spec
- Fixed `MergeLists` shutting down prematurely when an input closes while others still have data

## Version 0.7.5
### Features
- Added `Choice` plumbing node
- Added `ContentValidator`

### Changes
- Added type information to serialized output

### Fixes
- Fixed ordering for serialized output
- Casting for scratch path spec

## Version 0.7.4
### Changes
- Added option to specify scratch at workflow level

### Fixes
- Install package-based config if available

## Version 0.7.3
### Features
- Added `IndexDistribute`
- Added `IntegerMap`
- Added `FileBuffer`

### Changes
- Improved typing for dict utility functions
- Improved visualization colors
- Improved port activity determination for Multiplex
- Cleaned up workflow serialization

### Fixes
- Fixed typechecking of files passed as strings

## Version 0.7.2
### Fixes
- Fixed paths passed to inputs as parameters in a separate JSON failing to be cast

## Version 0.7.1
### Features
- Allowed mapping of inputs in serialized workflows

### Fixes
- Node list output is now sorted alphabetically

## Version 0.7.0
### Features
- Added diagrams to node documentation
- Added multiple new plumbing nodes (`TimeDistribute`, `MergeLists`, `CopyEveryNIter`)
- Node preparation is now cached, avoiding multiple dependency lookups
- `FileParameter` will now attempt casting strings to paths
- Allowed caching in `MultiInput`

### Changes
- Job queues will now be kept saturated
- Deprecated static `MultiPort`s
- Environment variables in interpreter specification are now expanded
- Split `TestRig.setup_run` for explicit use with variable outputs

### Fixes
- Fixed incorrect job submission counts
- Fixed typing issues in `TestRig`
- Added proper shutdown for `Multiplex`

## Version 0.6.2
### Features
- Interpreter - script pairs can now be non-path commands
- Added option to use list of paths for FileParameters

### Fixes
- Updated guide + dev instructions

## Version 0.6.1
### Features
- Added package directory as a search path for global config

### Changes
- Made class tags private

## Version 0.6.0
### Features
- Added send and receive hook support
- Added component tagging option

### Changes
- Config dependencies are now converted to absolute paths
- Removed init files causing problems with contrib
- Refactored execution to use correct logging

### Fixes
- Expanded test coverage
- Fix for `_prepare` calls with missing interpreter
- Fix for premature channel flush when handling converging data streams

## Version 0.5.1
### Features
- Added queue option to `JobResourceConfig`
- Added option to explicitly prefer batch submission

### Changes
- Warning when receiving multiple times from the same port without looping
- Added warning for single char custom batch attributes
- Job submission will now only submit n_jobs if larger than max_jobs
- Improved file validation, will now wait for files until timeout
- Changed handling of flags to explicit `--flag` / `--no-flag`
- `prepare()` no longer requires a call to the parent method

### Fixes
- Fix for receive not recognising optional unconnected ports
- Fixed looped nodes not handling cached input files properly
- Fix for `Workflow.from_dict` not recognizing input setting
- More robust batch job submissions
- Fixed occassional deadlocks under high logging loads
- Fixed `Return` nodes potentially freezing looped workflows

## Version 0.5.0
### Features
- Set parameters are logged at workflow start
- Added asynchronous command execution
- It is now possible to map free inputs on the workflow level
- Added checks for common graph construction problems
- Added support for CUDA MPS to run multiple processes on one GPU

### Changes
- Set default batch polling interval to 120s
- Added functionality to skip node execution if all parameters are unset / optional
- `Void` can now take any number of inputs
- Dynamic workflow creation is now possible using `expose`
- Added `working_dir` option to `run_command`
- Improved workflow status reporting
- Custom job attributes are now formatted correctly based on batch system
- Status updates now show full path for nodes in subgraphs with duplicate names

### Fixes
- Fixed missing cleanup when using relative scratch directory
- Avoid error when specifying duplicate `loop=True` parameter
- Fixed `typecheck` not handling dictionaries properly
- Fixed `common_parent` not breaking after divergence
- Fixed looped nodes not sending the correct file when dealing with identical names
- Temporary fix for paths not being parsed from YAML files

## Version 0.4.1
### Features
- Added command inputs to node `run_multi` method
- `FileChannel` can now send and receive dicts of files

### Changes
- Changed `exclusive_use` for batch submission to `False` by default
- Changed required Python to 3.10 to avoid odd beartype behaviour
- Documentation cleanup

### Fixes
- Fixed missing documentation propagation for inherited nodes
- Fixed `MultiPort` not being mapped properly in subgraphs
- Removed weird error code check
- Added missing `default` property to MultiInput
- Fixed misbehaving subgraph looping test

## Version 0.4.0
### Breaking changes
- Refactored looping system, only way to loop a node now is to use `loop=True`
- Removed dynamic interface building

### Features
- Allowed setting walltime in per-job config

### Changes
- Lowered logging noise
- Looping is now inherited from subgraphs
- Interface mapping sets attribute by default

### Fixes
- Fixed error in building docs
- Fixed validation failures not showing command output
- Fixed tests to match parameterization requirement
- Fix for incorrect walltime parsing for batch submission
- Throw a proper error when mapping ports with existing names
- More verbose message for missing connection

## Version 0.3.3
### Features
- Added `--parameters` commandline option to override workflow parameters using a JSON file
- Added timeout option to `run_command`
- Added simple multiple file I/O nodes

### Changes
- Unparameterized generic nodes will now cause graph construction to fail
- `FileParameter` will now cast strings to `Path` objects when setting
- Maize is now a proper namespace package

### Fixes
- Fixed cascading generic nodes sometimes not passing files correctly
- Fixed overeager parameter checks for serialized workflows
- Fixed bug preventing `run_multi` from running without `batch_options`

## Version 0.3.2
### Changes
- Updated dependencies
- Added contribution guidelines
- Prepared for initial public release

## Version 0.3.1
### Features
- Added mode option to `MultiPort`
- Added `custom_attributes` option for batch jobs

### Changes
- Better batch submission tests
- Batch submission now only requires `batch_options`

### Fixes
- Fixed resource management issue
- Fixed file copy issues with the `LoadFile` node
- Fixed off-by-one in finished job count
- Various improvements to `parallel`

## Version 0.3
### Features
- Added pre-execution option for `run_command`
- Allow setting default parameters in global config
- Added interface / component serialization

### Changes
- `Input` can now also act as a `Parameter`
- `Input` can now cache its data
- Overhauled `FileChannel`, now supports lists of files

### Fixes
- Improved type checking for paths
- Fix for PSI/J execution freezing when using `fork`
- Resolved occasional node looping issues
- `SaveFile` now handles directories properly
- Inherited nodes now set datatypes properly
- Better missing graphviz information
- Improved working directory cleanup
- `LoadFile` now never moves files
- `run_command` now parses whitespace / strings correctly
- Missing config warnings

## Version 0.2
### Features
- Can now submit jobs to arbitrary resource manager systems (SLURM, PBS, etc)
- `run_command` now accepts `stdin` command input (Thanks Marco)
- Added tool to convert from functions to nodes
- Added experimental node parallelization macro
- Added utility nodes for batching data, with example workflow
- Added `Barrier` node for conditional sending, `Yes` as an equivalent to the Unix command
- Workflow visualization improvements (depth limit, node status)

### Changes
- All execution is now performed with Exaworks PSI/J (new `psij-python` dependency)
- Dynamic typechecking now uses `beartype`
- Parameters with no default value will cause an error if not set to optional
- Status summaries now show approximate number of items in channel
- Channel size can be set globally for the whole workflow

### Fixes
- Many fixes in dynamic typechecking
- More informative error messages
- Fixed a race condition during certain executions
- Fixed an issue where channel data would get flushed in long-running workflows
- Fixed issues relating to Graphviz
- Lowered chances of zombie / orphan processes
- Fixed issue where the config would not get read properly

## Version 0.1
Initial release.
