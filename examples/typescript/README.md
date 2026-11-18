# TypeScript example

```sh
(cd ../../typescript && npm install && npm run build)
npm install
GROW_AWS_CLOUDWATCH_LOGS_URL=http://127.0.0.1:<port> GROW_AWS_CLOUDWATCH_LOGS_TOKEN=... npm start
```

Replace the `file:` dependency in `package.json` with a version (`npm install @grow-codes/aws-cloudwatch-logs`)
to run it against the published package.
