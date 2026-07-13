const BARK_PUSH_URL = "https://api.day.app/push";
const BARK_TIMEOUT_MS = 5_000;

type BarkFetch = typeof fetch;

export type BarkNotification = {
  body: string;
  title: string;
};

export type BarkNotifier = {
  enabled: boolean;
  send(notification: BarkNotification): Promise<void>;
};

function barkError(message: string): Error {
  return new Error(`[bark] ${message}`);
}

export function createBarkNotifier({
  fetchImpl = globalThis.fetch,
  key = process.env.CODEX_WEB_BARK_KEY?.trim(),
}: {
  fetchImpl?: BarkFetch;
  key?: string;
} = {}): BarkNotifier {
  if (!key) {
    return {
      enabled: false,
      async send(): Promise<void> {},
    };
  }

  return {
    enabled: true,
    async send({ body, title }): Promise<void> {
      const response = await fetchImpl(BARK_PUSH_URL, {
        method: "POST",
        headers: {
          "content-type": "application/json; charset=utf-8",
        },
        body: JSON.stringify({
          body,
          device_key: key,
          group: "codex-web",
          title,
        }),
        signal: AbortSignal.timeout(BARK_TIMEOUT_MS),
      });

      if (!response.ok) {
        throw barkError(`request failed with status ${response.status}`);
      }

      const contentType = response.headers.get("content-type") ?? "";
      if (!contentType.includes("application/json")) {
        return;
      }

      const result = (await response.json()) as { code?: unknown };
      if (typeof result.code === "number" && result.code !== 200) {
        throw barkError(`service rejected the notification (${result.code})`);
      }
    },
  };
}

export const barkNotifier = createBarkNotifier();
