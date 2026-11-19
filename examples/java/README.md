# Java examples

Install the package into your local Maven repository, point it at a running Amazon CloudWatch Logs and run an
example:

```bash
make -C ../../java pack            # builds and installs codes.grow.packages:aws-cloudwatch-logs
export GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:5000
export AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...   # standard AWS chain
mvn -q compile exec:java                                                  # BasicExample
mvn -q compile exec:java -Dexec.mainClass=codes.grow.packages.cloudwatchlogs.examples.AsyncExample
```

Against the published package, set `-Dsdk.version=<version>` (or edit `pom.xml`) instead of
installing it locally.

| Class | Shows |
| - | - |
| `BasicExample` | blocking client: health, ping, log group + stream, putLogEvents, getLogEvents, filterLogEvents, Logs Insights, EMF, log client + logger, search |
| `AsyncExample` | `…Async` methods, combining futures, cancellation |
| `HooksAndLoggingExample` | request hooks, a logger, retries and timeouts |
