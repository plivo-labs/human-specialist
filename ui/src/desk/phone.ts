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

export type Phone = { answer: (uuid: string) => Promise<void>; reject: (uuid: string) => void; hangup: () => void; mute: (on: boolean) => void; carries: (cid: string) => boolean; dtmf: (cid: string, digit: string) => string; login: (token: string) => void };

export const KEYS = "123456789*0#";
export const key = (pressed: string) => (pressed.length === 1 && KEYS.includes(pressed) ? pressed : null);

type Held = { uuid: string; conversation?: string } | null;
type Call = { getCallUUID: () => string | null; isConnected: () => boolean | null; sendDtmf: (digit: string) => void };
// The SDK holds one call; the ring this browser answered named the conversation that call belongs to.
export const carries = (client: Call, held: Held, cid: string) => !!held && held.conversation === cid && client.getCallUUID() === held.uuid;
// sendDtmf returns nothing and drops a digit with only a log line, so what the desk can see is checked first.
export const tone = (client: Call, held: Held, cid: string, online: boolean, digit: string) =>
  !carries(client, held, cid) ? "This browser no longer holds this call." : !online || !client.isConnected() ? "This browser lost its connection to the phone service." : (client.sendDtmf(digit), "");

type Handlers = {
  state: (state: "ready" | "failed", why?: string) => void;
  ring: (r: Ring) => void;
  gone: (uuid: string) => void;
  answered: (uuid: string) => void;
};

const headers = (raw: Record<string, string> = {}) =>
  Object.fromEntries(Object.entries(raw).map(([k, v]) => [k.toLowerCase().replace(/^x-ph-hs/, ""), decodeURIComponent(v)]));

const BLOCKED = "The browser blocked the microphone. Allow it for this page from the address bar, then reload.";

const GRANTED = "hs.microphone";
const remembered = () => { try { return localStorage.getItem(GRANTED) === "granted"; } catch { return false; } };
function remember(granted: boolean) {
  try {
    if (granted) localStorage.setItem(GRANTED, "granted");
    else localStorage.removeItem(GRANTED);
  } catch (e) {
    console.warn("This browser keeps no note that the microphone is allowed, so the desk asks again next time", e);
  }
}

// Ask only while the browser has not decided: the SDK holds a ring until the microphone is allowed, and the ring times out.
// Without the Permissions API, a note on this device stands for the browser's answer.
export async function askForMicrophone(): Promise<string> {
  const decided = await navigator.permissions?.query({ name: "microphone" as PermissionName }).catch(() => null);
  if (decided?.state === "denied") return (remember(false), BLOCKED);
  if (decided?.state === "granted" || (!decided && remembered())) return "";
  try {
    const mic = await navigator.mediaDevices.getUserMedia({ audio: true });
    mic.getTracks().forEach((t) => t.stop());
    remember(true);
    return "";
  } catch (e) {
    const name = e instanceof Error ? e.name : "";
    if (name === "NotAllowedError") return (remember(false), BLOCKED);
    if (name === "NotFoundError") return "This computer has no microphone. Plug one in, then reload.";
    return `The microphone did not start (${e instanceof Error ? e.message : e}). Close other apps that use it, then reload.`;
  }
}

export async function startPhone(token: string, on: Handlers): Promise<Phone | null> {
  const blocked = await askForMicrophone();
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
  let held: Held = null;
  client.on("onLogin", () => on.state("ready"));
  client.on("onLoginFailed", (cause: string) => on.state("failed", `Plivo refused the phone sign-in: ${cause}.`));
  client.on("onMediaPermission", (e: any) => e?.status === "failure" && on.state("failed", "The browser blocked the microphone. Allow it for this page, then reload."));
  client.on("onIncomingCall", (from: string, raw: Record<string, string>, info: any) => on.ring({ uuid: uuid(info), from, at: Date.now(), ...headers(raw) }));
  client.on("onIncomingCallCanceled", (info: any) => {
    settle(uuid(info), "The caller hung up before the call connected.");
    on.gone(uuid(info));
  });
  client.on("onCallAnswered", (info: any) => {
    held = { uuid: uuid(info), conversation: headers(info?.extraHeaders).conversation };
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
    carries: (cid) => carries(client, held, cid),
    dtmf: (cid, digit) => tone(client, held, cid, navigator.onLine, digit),
    login: (t) => client.loginWithAccessToken(t),
  };
}
