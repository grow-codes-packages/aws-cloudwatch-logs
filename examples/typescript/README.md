# TypeScript example

```sh
(cd ../../typescript && npm install && npm run build)
npm install
AWS_REGION=us-east-1 AWS_PROFILE=dev npm start
```

The example creates a temporary log group `grow-sdk-example/<timestamp>` (retention 1 day) and deletes
it at the end; set `EXAMPLE_LOG_GROUP` to use an existing group instead (it is then kept). Against
the Docker emulator set `GROW_AWS_CLOUDWATCH_LOGS_URL` and the bootstrap credentials.

Replace the `file:` dependency in `package.json` with a version (`npm install @grow-codes/aws-cloudwatch-logs`)
to run it against the published package.
