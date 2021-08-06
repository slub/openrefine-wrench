# openrefine-wrench

> OpenRefine (previously Google Refine) is a powerful tool for working with messy data: cleaning it; transforming it from one format into another; and extending it with web services and external data.

The openrefine-wrench commandline tool supports the basic openrefine operations:
* create openrefine project (csv and xml source data only)
* apply rules to openrefine project
* export modified openrefine project (csv export only)
* delete openrefine project

The main feature of openrefine-wrench is the orchestration of these operations over multiple processes to handle multiple input files in separte openrefine projects.

## usage
***

### handle multiple input files in separte openrefine projects

```
$ openrefine-wrench --help                  
Usage: openrefine-wrench [OPTIONS]

  ...

Options:
  --host TEXT                     openrefine host  [required]
  --port TEXT                     openrefine port (default to 3333)
                                  [required]
  --source-dir TEXT               openrefine source data dir  [required]
  --export-dir TEXT               openrefine export data dir  [required]
  --source-format [xml|csv]       openrefine source data format  [required]
  --encoding TEXT                 openrefine source data encoding (default to
                                  UTF-8)  [required]
  --record-path TEXT              record path (only applicable in conjunction
                                  with xml source format)
  --mappings-file TEXT            openrefine mappings file  [required]
  --max-workers INTEGER           number of parallel processed openrefine
                                  projects  [required]
  --log-level [DEBUG|INFO|WARN|ERROR|OFF]
                                  log level (default INFO)
  --help                          Show this message and exit.
```

### create single openrefine project

```
$ openrefine-wrench-create --help 
Usage: openrefine-wrench-create [OPTIONS]

  ...

Options:
  --host TEXT                     openrefine host  [required]
  --port TEXT                     openrefine port (default to 3333)
                                  [required]
  --source-file TEXT              openrefine source file  [required]
  --source-format [xml|csv]       openrefine source data format  [required]
  --project-name TEXT             openrefine project name  [required]
  --encoding TEXT                 openrefine source data encoding (default to
                                  UTF-8)  [required]
  --record-path TEXT              record path (only applicable in conjunction
                                  with xml source format)
  --log-level [DEBUG|INFO|WARN|ERROR|OFF]
                                  log level (default INFO)
  --help                          Show this message and exit.
```

### apply rules to single openrefine project

```
$ openrefine-wrench-apply --help 
Usage: openrefine-wrench-apply [OPTIONS]

  ...

Options:
  --host TEXT                     openrefine host  [required]
  --port TEXT                     openrefine port (default to 3333)
                                  [required]
  --project-id TEXT               openrefine project id  [required]
  --mappings-file TEXT            openrefine mappings file  [required]
  --log-level [DEBUG|INFO|WARN|ERROR|OFF]
                                  log level (default INFO)
  --help                          Show this message and exit.
```

### export single modified openrefine project

```
$ openrefine-wrench-export --help 
Usage: openrefine-wrench-export [OPTIONS]

  ...

Options:
  --host TEXT                     openrefine host  [required]
  --port TEXT                     openrefine port (default to 3333)
                                  [required]
  --export-file TEXT              openrefine export file  [required]
  --project-id TEXT               openrefine project id  [required]
  --log-level [DEBUG|INFO|WARN|ERROR|OFF]
                                  log level (default INFO)
  --help                          Show this message and exit.
```

### delete single openrefine project

```
$ openrefine-wrench-delete --help 
Usage: openrefine-wrench-delete [OPTIONS]

  ...

Options:
  --host TEXT                     openrefine host  [required]
  --port TEXT                     openrefine port (default to 3333)
                                  [required]
  --project-id TEXT               openrefine project id  [required]
  --log-level [DEBUG|INFO|WARN|ERROR|OFF]
                                  log level (default INFO)
  --help                          Show this message and exit.
```

## installation
***

Install via pip, e.g.:

```
$ python3 -m pip install git+https://github.com/slub/openrefine-wrench --user
```

## licenses
***

This repository is licensed under the [MIT License](https://github.com/slub/openrefine-wrench/blob/master/LICENSE).