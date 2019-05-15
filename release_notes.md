## Release Notes

### 2.0.11-SNAPSHOT

A lot of cumulative work has made it into this release.

#### Command Line Enhancements:
- 'silent' option added to suppress output.  Helpful when using the 'run' option.
- 'run' option to 'run' a file with a list of commands.
- 'stdin' option added so commands could be piped into the cli and run under the same session.
- 'debug' and 'verbose' options added to increase visibility of actions being run.

#### Functional Enhancements:
Each function has a help option:
`help <function>`

##### Core HDFS Commands (added)
- `count`
- `test`

##### lsp (ls plus)
- filter support (regex using Java Pattern matching to find matching paths in search)
- formatting options
- output to hdfs option
- control over recursiveness with 'depth' option
- comment option to enhance output.  Nice to be able to carry extra metadata along in a pipeline.


### 1.0.0-SNAPSHOT (in-progress)

This release is the first for `hadoopcli` and is an extension to (replacement of) `hdfs-cli` found [here](https://github.com/dstreev/hdfs-cli)

Building on release [2.3.2-SNAPSHOT](https://github.com/dstreev/hdfs-cli) of `hdfscli`, this version starts to integrate with the`Job History Server`, `YARN` and `ATS`.  Hence to change from `hdfscli` to `hadoopcli`.
