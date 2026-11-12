export type Ring = {
  uuid: string;
  from: string;
  at: number;
  kind?: string;
  offer?: string;
  conversation?: string;
  autoanswer?: string;
  team?: string;
  fromagent?: string;
};

export type Phone = { answer: (uuid: string) => Promise<void>; reject: (uuid: string) => void; hangup: () => void; mute: (on: boolean) => void; login: (token: string) => void };

type Handlers = {
  state: (state: "ready" | "failed", why?: string) => void;
  ring: (r: Ring) => void;
  gone: (uuid: string) => void;
  answered: (uuid: string) => void;
};

const headers = (raw: Record<string, string> = {}) =>
  Object.fromEntries(Object.entries(raw).map(([k, v]) => [k.toLowerCase().replace(/^x-ph-hs/, ""), decodeURIComponent(v)]));

// Asked once at sign-in: the SDK otherwise holds a ring until the agent allows the microphone, and the ring times out.
async function microphone(): Promise<string> {
  try {
    const mic = await navigator.mediaDevices.getUserMedia({ audio: true });
    mic.getTracks().forEach((t) => t.stop());
    return "";
  } catch (e) {
    const name = e instanceof Error ? e.name : "";
    if (name === "NotAllowedError") return "The browser blocked the microphone. Allow it for this page from the address bar, then reload.";
    if (name === "NotFoundError") return "This computer has no microphone. Plug one in, then reload.";
    return `The microphone did not start (${e instanceof Error ? e.message : e}). Close other apps that use it, then reload.`;
  }
}

export async function startPhone(token: string, on: Handlers): Promise<Phone | null> {
  const blocked = await microphone();
  if (blocked) {
    on.state("failed", blocked);
    return null;
  }
  let Plivo;
  try {
    Plivo = (await import("plivo-browser-sdk")).default;
  } catch {
    on.state("failed", "The phone code did not load. Reload the page.");
    return null;
  }
  const client = new Plivo({ debug: "ERROR", permOnClick: true, closeProtection: true }).client;
  const waiting = new Map<string, { resolve: () => void; reject: (e: Error) => void }>();
  const settle = (uuid: string, error?: string) => {
    const w = waiting.get(uuid);
    waiting.delete(uuid);
    if (w) error ? w.reject(new Error(error)) : w.resolve();
  };
  const uuid = (info: any) => String(info?.callUUID ?? "");
  client.on("onLogin", () => on.state("ready"));
  client.on("onLoginFailed", (cause: string) => on.state("failed", `Plivo refused the phone sign-in: ${cause}.`));
  client.on("onMediaPermission", (e: any) => e?.status === "failure" && on.state("failed", "The browser blocked the microphone. Allow it for this page, then reload."));
  client.on("onIncomingCall", (from: string, raw: Record<string, string>, info: any) => on.ring({ uuid: uuid(info), from, at: Date.now(), ...headers(raw) }));
  client.on("onIncomingCallCanceled", (info: any) => {
    settle(uuid(info), "The caller hung up before the call connected.");
    on.gone(uuid(info));
  });
  client.on("onCallAnswered", (info: any) => {
    settle(uuid(info));
    on.answered(uuid(info));
  });
  client.on("onCallFailed", (cause: string, info: any) => settle(uuid(info), `The call failed: ${cause}.`));
  client.on("onCallTerminated", (_: unknown, info: any) => on.gone(uuid(info)));
  client.loginWithAccessToken(token);
  return {
    answer: (id) =>
      new Promise((resolve, reject) => {
        waiting.set(id, { resolve, reject });
        setTimeout(() => settle(id, "The phone did not connect within 15 seconds."), 15000);
        client.answer(id, "letring");
      }),
    reject: (id) => client.reject(id),
    hangup: () => client.hangup(),
    mute: (muted) => (muted ? client.mute() : client.unmute()),
    login: (t) => client.loginWithAccessToken(t),
  };
}
