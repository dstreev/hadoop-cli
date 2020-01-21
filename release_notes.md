## Release Notes

### 2.1.0-SNAPSHOT

The API release.  Combined / Collapsed stemshell into this project and created a more consumable API interface that can be use externally.  See: [Test API for Example](./src/test/java/com/streever/hadoop/api/TestApi.java)

[ISSUE 11](https://github.com/dstreev/hadoop-cli/issues/11)
[ISSUE 12](https://github.com/dstreev/hadoop-cli/issues/12)
[ISSUE 13](https://github.com/dstreev/hadoop-cli/issues/13)
[ISSUE 14](https://github.com/dstreev/hadoop-cli/issues/14)

### 2.0.19-SNAPSHOT

Been a while since I've updated this.  Lots of fixes and enhancements.  Review the issues below for details:

[ISSUE 6](https://github.com/dstreev/hadoop-cli/issues/6)
[ISSUE 7](https://github.com/dstreev/hadoop-cli/issues/7)
[ISSUE 8](https://github.com/dstreev/hadoop-cli/issues/8)
[ISSUE 9](https://github.com/dstreev/hadoop-cli/issues/9)
[ISSUE 10](https://github.com/dstreev/hadoop-cli/issues/10)

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

This release is the first for `hadoopcli` and is an extension to (replacement of) `hdfs-cli` testFound [here](https://github.com/dstreev/hdfs-cli)

Building on release [2.3.2-SNAPSHOT](https://github.com/dstreev/hdfs-cli) of `hdfscli`, this version starts to integrate with the`Job History Server`, `YARN` and `ATS`.  Hence to change from `hdfscli` to `hadoopcli`.
