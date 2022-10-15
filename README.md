## Introduction
This is the README file for the `github events analysis` project.

In this project I will aggregate events extracted from GitHub in order to
extract metrics to allow us to get a picture of what repositories
are used the most and who is using them.

## Details
The language chosen for this project is `python`. Pyspark will also be used,
since large amounts of data are expected.

The data chosen for this project corresponds to January 2022 and they can be
accessed through https://www.githubarchive.org/.

## Branches
The `master` branch has been created in order to hold this `README`
file.

The procedure to follow is the usual: create a new feature branch every time
you want to add a feature to the code and go through the PR process.

## Workflow
The metrics provided by this project are:
- User aggregation metrics.
- Repository aggregation metrics.

### Inputs
There are four input parameters:
- Initial date we want to get the metrics for.
- Final date we want to get the metrics for (we will get the metrics for all
the days in between the initial and final dates).
- Path where we want to get the user aggregated metrics.
- Path where we want to get the repository aggregated metrics.

### Get and format the data
The first step in the workflow is about reading and formatting the data. The
reason for this is that the initial dataset has a very complex structure and
since we are not interested in all the input columns to get our metrics, we
can simply select the columns we are interested in.

The user has provided an initial and final dates, so all the files between 
these two dates are read, then the convenient columns are selected and then
all the datasets are put together for the transformations.

### Getting the date column
The input dataset contains information about the datetime at hour level. To 
be able to aggregate by date, we create a new column that only considers the
date, not the time.

### User aggregation metrics
The next step is to calculate the user-aggregated metrics. These metrics are:
- Created issues.
- Created PRs.
- Starred projects.

### Repository aggregation metrics
The next step is to calculate the repository-aggregated metrics. These metrics
are:
- Number of users that starred the repository.
- Number of users that forked the repository.
- Number of created issues.
- Number of created PRs.

### Writing the output
Once the metrics are calculated, the next step is to write them, partitioned
by date, in the paths that the user provided as input.


## How do I run the code?