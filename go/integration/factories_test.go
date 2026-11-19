package integration

import (
	"context"
	"strings"
	"testing"

	cloudwatchlogs "github.com/grow-codes-packages/aws-cloudwatch-logs/go"
	"github.com/grow-codes-packages/aws-cloudwatch-logs/go/testkit"
)

// capabilities the deployment under test offers (see conformance "requires").
// "version" is left out: CloudWatch Logs exposes no server version
// (serverVersion is UNSUPPORTED, see docs/compatibility.md).
var capabilities = []string{"query"}

// categories runs "core" plus this service's categories.
func categories() []string {
	return append([]string{"core"}, strings.Split("logging", ",")...)
}

// factories builds the conformance targets. Every logging scenario writes to its
// own log group (created here, deleted afterwards) so runs never see each other.
func factories(t *testing.T) testkit.Factories {
	t.Helper()
	return testkit.Factories{
		"core": testkit.CoreFactory(func(context.Context) (testkit.CoreTarget, error) {
			return cloudwatchlogs.NewClientFromEnv()
		}),
		"logging": func(ctx context.Context, env *testkit.Env) (any, error) {
			group := "grow-conformance-go/" + env.Run
			cfg, err := cloudwatchlogs.ConfigFromEnvWith(cloudwatchlogs.Config{LogGroup: group, LogStream: "conformance-" + env.Run}, nil)
			if err != nil {
				return nil, err
			}
			client, err := cloudwatchlogs.NewClient(cfg)
			if err != nil {
				return nil, err
			}
			if _, err := client.EnsureLogGroup(ctx, group, cloudwatchlogs.EnsureLogGroupOptions{}); err != nil {
				_ = client.Close()
				return nil, err
			}
			env.AddCleanup(func() {
				_, _ = client.DeleteLogGroup(context.Background(), group, cloudwatchlogs.DeleteOptions{IgnoreMissing: true})
				_ = client.Close()
			})
			return testkit.LoggingFactory(func(_ context.Context, opts cloudwatchlogs.BatchOptions) (testkit.LoggingTarget, error) {
				lc, err := client.LogClient(cloudwatchlogs.LogClientConfig{LogClientOptions: cloudwatchlogs.LogClientOptions{Batch: opts}})
				if err != nil {
					return testkit.LoggingTarget{}, err
				}
				q, err := client.LogQueryClient("")
				if err != nil {
					return testkit.LoggingTarget{}, err
				}
				return testkit.LoggingTarget{Client: lc, Query: q}, nil
			})(ctx, env)
		},
	}
}
