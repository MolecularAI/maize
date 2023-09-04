# CHANGELOG

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
