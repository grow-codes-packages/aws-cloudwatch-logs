# TypeScript integration tests

A consumer project: it installs the packed `@grow-codes/aws-cloudwatch-logs` tarball (never the source tree)
and runs the integration and conformance tests with `node --test` against a real Amazon CloudWatch Logs.

```sh
cd typescript && make pack            # writes dist/grow-codes-aws-cloudwatch-logs-<version>.tgz
cd integration && npm run install-package
GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:<port> AWS_REGION=us-east-1 \
  AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... CLOUDWATCH_LOG_GROUP=... npm test
```

In CI and locally with Docker this runs inside `docker/Dockerfile.typescript`
(`make test-docker LANGS=typescript` from the repository root), which reads `/bootstrap/env`.
