import { cap, explain } from "./format";
import type { Session } from "./types";

declare global {
  interface Window {
    HS_URL?: string;
  }
}

export const BASE = (window.HS_URL ?? "").replace(/\/$/, "");

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly details?: Record<string, unknown>,
  ) {
    super(message);
  }
}

export async function api<T = unknown>(method: string, path: string, body?: unknown, bearer = held().key ?? held().session?.agent_token ?? ""): Promise<T> {
  let res: Response;
  try {
    res = await fetch(BASE + path, {
      method,
      headers: { ...(bearer && { Authorization: `Bearer ${bearer}` }), ...(body !== undefined && { "Content-Type": "application/json" }) },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch {
    const cors = BASE ? `, and that its CORS_ALLOWED_ORIGINS lists ${location.origin}` : "";
    throw new ApiError(0, "unreachable", `Cannot reach the server at ${BASE || location.origin}. Check that it is running${cors}.`);
  }
  if (res.status === 204) return null as T;
  const data = await res.json().catch(() => null);
  if (res.status === 401 && bearer && bearer === held().key) {
    const why = "This organization key no longer works. Sign in with a current one.";
    hold({ key: null, signedOut: why });
    if (!held().session) location.hash = "/signin";
    throw new ApiError(401, data?.error?.code ?? "unauthorized", why);
  }
  if (res.status === 401 && bearer === held().session?.agent_token) reloadToSignIn();
  if (!res.ok) {
    throw new ApiError(res.status, data?.error?.code ?? `http_${res.status}`, explain(data?.error?.message ?? `The server answered ${res.status}`, data?.error?.details), data?.error?.details);
  }
  return data as T;
}

export const asAgent = <T = unknown>(method: string, path: string, body?: unknown) => api<T>(method, path, body, held().session?.agent_token ?? "");

export type Page<T> = { data: T[]; next_cursor: string | null };

export async function every<T>(page: (cursor: string) => Promise<Page<T>>): Promise<T[]> {
  const out: T[] = [];
  for (let cursor = ""; ; ) {
    const p = await page(cursor);
    out.push(...p.data);
    if (!p.next_cursor) return out;
    cursor = `&cursor=${encodeURIComponent(p.next_cursor)}`;
  }
}

export function reason(e: unknown): string {
  if (!(e instanceof ApiError)) return e instanceof Error ? e.message : String(e);
  const text = cap(e.message);
  return `${text}${/[.!?]$/.test(text) ? "" : "."}${e.status ? ` (${e.status} ${e.code})` : ""}`;
}

export const store = {
  get: <T>(name: string): T | null => JSON.parse(sessionStorage.getItem(`hs.${name}`) ?? "null"),
  set: (name: string, value: unknown) =>
    value == null ? sessionStorage.removeItem(`hs.${name}`) : sessionStorage.setItem(`hs.${name}`, JSON.stringify(value)),
};

export const held = () => ({ key: store.get<string>("key"), session: store.get<Session>("session") });
export const hold = (c: { key?: string | null; session?: Session | null; signedOut?: string | null }) => Object.entries(c).forEach(([k, v]) => store.set(k, v));
const ENDED = "Your session ended, so calls no longer ring here. Sign in again.";
export function reloadToSignIn(why: string | null = ENDED) {
  hold({ session: null, signedOut: why });
  location.hash = "/signin";
  location.reload();
}

export async function signOut() {
  if (held().session) await asAgent("DELETE", `/api/v1/me/sessions`).catch((e) => console.warn("Signing out failed", e));
  hold({ key: null });
  reloadToSignIn(null);
}

export const mint = (agentId: string) => api<Session>("POST", `/api/v1/agents/${agentId}/sessions`);
export const renew = () => asAgent<Session>("POST", `/api/v1/me/sessions`);
export const renewals = new EventTarget();
