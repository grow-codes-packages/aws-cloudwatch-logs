// Service-owned: AWS error code → SPEC error class mapping (API.md §1.1).

import { describe, expect, it } from "vitest";
import { AWS_ERROR_CODES, ConflictError, awsErrorCode, mapAwsError } from "../src/index.js";

function awsErr(name: string, status?: number, extra: Record<string, unknown> = {}): Error {
  return Object.assign(new Error(`message for ${name}`), {
    name,
    ...(status === undefined ? {} : { $metadata: { httpStatusCode: status, requestId: "rid" } }),
    ...extra,
  });
}

describe("mapAwsError", () => {
  it("maps every listed AWS code", () => {
    for (const [code, grow] of Object.entries(AWS_ERROR_CODES)) {
      const e = mapAwsError("op", awsErr(code, 400));
      expect(e.code).toBe(grow);
      if (code !== "CredentialsProviderError")
        expect(e.message).toMatch(new RegExp(`^op: .+: ${code} \\(HTTP 400\\)$`));
      expect(e.requestId).toBe("rid");
    }
  });

  it("strips namespaces and sets retryability overrides", () => {
    expect(awsErrorCode({ name: "com.amazonaws.logs#ThrottlingException" })).toBe(
      "ThrottlingException",
    );
    expect(awsErrorCode({ Code: "X" })).toBe("X");
    expect(awsErrorCode(null)).toBeUndefined();
    expect(awsErrorCode({})).toBeUndefined();
    expect(mapAwsError("op", awsErr("ns#ThrottlingException", 400))).toMatchObject({
      code: "RATE_LIMITED",
      retryable: true,
    });
    expect(mapAwsError("op", awsErr("OperationAbortedException", 400))).toMatchObject({
      code: "CONFLICT",
      retryable: true,
    });
    expect(mapAwsError("op", awsErr("ResourceAlreadyExistsException", 400))).toMatchObject({
      code: "CONFLICT",
      retryable: false,
    });
    expect(mapAwsError("op", awsErr("SessionTimeoutException"))).toMatchObject({
      code: "TIMEOUT",
      retryable: true,
    });
  });

  it("falls back to the HTTP status and the 403 body rule", () => {
    const cases: [number, string][] = [
      [401, "AUTHENTICATION"],
      [404, "NOT_FOUND"],
      [408, "TIMEOUT"],
      [504, "TIMEOUT"],
      [429, "RATE_LIMITED"],
      [500, "UNAVAILABLE"],
      [418, "VALIDATION"],
    ];
    for (const [status, code] of cases)
      expect(mapAwsError("op", awsErr("SyntaxError", status)).code).toBe(code);
    const xml = (text: string) =>
      awsErr("SyntaxError", 403, {
        $response: { statusCode: 403, body: new TextEncoder().encode(text) },
      });
    expect(mapAwsError("op", xml("<Code>SignatureDoesNotMatch</Code>")).code).toBe(
      "AUTHENTICATION",
    );
    expect(mapAwsError("op", xml("<Code>AccessDenied</Code>")).code).toBe("PERMISSION_DENIED");
    expect(
      mapAwsError(
        "op",
        awsErr("Weird", 403, { $responseBodyText: "The security token is invalid" }),
      ).code,
    ).toBe("AUTHENTICATION");
    expect(
      mapAwsError("op", awsErr("Weird", undefined, { $response: { statusCode: 403, body: "x" } }))
        .code,
    ).toBe("PERMISSION_DENIED");
  });

  it("maps cancellation, credentials, region, timeouts and network errors", () => {
    expect(mapAwsError("op", awsErr("AbortError")).code).toBe("CANCELLED");
    expect(mapAwsError("op", awsErr("CredentialsProviderError")).code).toBe("AUTHENTICATION");
    expect(mapAwsError("op", new Error("Region is missing")).code).toBe("CONFIG");
    expect(mapAwsError("op", awsErr("TimeoutError")).code).toBe("TIMEOUT");
    expect(mapAwsError("op", Object.assign(new Error("t"), { code: "ETIMEDOUT" })).code).toBe(
      "TIMEOUT",
    );
    const net = new Error("fetch failed", {
      cause: Object.assign(new Error("refused"), { code: "ECONNREFUSED" }),
    });
    expect(mapAwsError("op", net)).toMatchObject({
      code: "UNAVAILABLE",
      message: "op: service unavailable: ECONNREFUSED",
    });
    expect(mapAwsError("op", "string error").code).toBe("UNAVAILABLE");
    const existing = new ConflictError("kept");
    expect(mapAwsError("op", existing)).toBe(existing);
  });

  it("never copies AWS messages and redacts known secrets from the cause", () => {
    const e = mapAwsError(
      "op",
      awsErr("InvalidParameterException", 400, { message: "event text sk-secret-1234" }),
      ["sk-secret-1234"],
    );
    expect(e.message).not.toContain("event text");
    expect((e.cause as Error).message).not.toContain("sk-secret-1234");
  });
});
