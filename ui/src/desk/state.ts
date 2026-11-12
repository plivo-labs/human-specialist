import type { Assignment, Conversation, Disposition, Envelope, Me, Message, PresenceStatus, Status } from "../types";
import type { Ring } from "./phone";

export type Offer = { id: string; cid: string; channel: string; from: string | null; to: string | null; queueId: string | null; via: string; ringFor: number | null; at: number };
export type Dial = { cid: string; to: string };
export type Transfer = { id: string; mode: "warm" | "cold"; label: string; status: string };
export type Line = {
  cid: string;
  conv: Conversation;
  phase: "live" | "wrap";
  monitor: string | null;
  started: number;
  until: number | null;
  held: boolean;
  via: string | null;
  consult: boolean;
  muted: boolean;
  paused: boolean;
  transfer: Transfer | null;
  stale?: boolean;
};

export type State = {
  me: Me;
  statuses: PresenceStatus[];
  dispositions: Disposition[];
  names: Record<string, string>;
  phone: { state: "connecting" | "ready" | "failed"; why: string };
  stream: "connecting" | "live" | "stopped";
  unreachable: boolean;
  offers: Offer[];
  notices: { id: string; missed: boolean; channel: string; from: string | null; strike?: number }[];
  rings: Ring[];
  lines: Line[];
  focus: string;
  dialing: Dial | null;
  feed: Envelope[];
};

export type Action =
  | { type: "phone"; state: State["phone"]["state"]; why?: string }
  | { type: "stream"; state: State["stream"] }
  | { type: "reachable"; ok: boolean }
  | { type: "renewed"; me: Me }
  | { type: "event"; e: Envelope }
  | { type: "ring"; ring: Ring }
  | { type: "ring-gone"; uuid: string }
  | { type: "line"; cid: string; conv: Conversation | null }
  | { type: "dialing"; dial: Dial }
  | { type: "patch"; cid: string; patch: Partial<Line> }
  | { type: "names"; names: Record<string, string> }
  | { type: "status"; status: Status }
  | { type: "statuses"; statuses: PresenceStatus[] }
  | { type: "decline"; offer: string; uuid: string }
  | { type: "dismiss"; id: string };

export const TEXT = new Set(["chat", "sms", "whatsapp", "email"]);
export const TASK = new Set(["task", "callback"]);

export const init = (me: Me, statuses: PresenceStatus[], dispositions: Disposition[]): State => ({
  me,
  statuses,
  dispositions,
  names: {},
  phone: { state: "connecting", why: "" },
  stream: "connecting",
  unreachable: false,
  offers: me.pending_offers.map((o) => ({ id: o.offer_id, cid: o.conversation_id, channel: o.channel, from: o.from, to: null, queueId: null, via: o.via, ringFor: (Date.parse(o.expires_at) - Date.parse(o.offered_at)) / 1000, at: Date.parse(o.offered_at) })),
  notices: [],
  rings: [],
  lines: [],
  focus: "",
  dialing: null,
  feed: [],
});

const ENDS_OFFER = new Set(["offer.accepted", "offer.rejected", "offer.missed", "offer.withdrawn"]);
const ENDS_DIAL = new Set(["call.answered", "call.failed", "conversation.closed"]);
const ENDS_TRANSFER = new Set(["completed", "failed", "cancelled"]);
const LINE_FLAGS: Record<string, Partial<Line>> = {
  "call.recording_paused": { paused: true },
  "call.recording_resumed": { paused: false },
  "call.ended": { held: false, muted: false, paused: false },
};

export const namesMe = (s: State, e: Envelope) => e.agent_id === s.me.id || e.data?.supervisor_id === s.me.id;
export const onLine = (s: State, e: Envelope) => s.lines.some((l) => l.cid === e.conversation_id);
export const onDial = (s: State, e: Envelope) => !!e.conversation_id && e.conversation_id === s.dialing?.cid;

function onEvent(s: State, e: Envelope): State {
  const d = e.data ?? {};
  let { offers, me, dialing, notices } = s;
  if (e.type === "offer.created" && namesMe(s, e))
    offers = [...offers, { id: d.offer_id, cid: e.conversation_id!, channel: d.channel, from: d.from, to: d.to, queueId: d.queue_id, via: d.via, ringFor: d.ring_timeout_sec, at: Date.now() }];
  const lost = offers.find((o) => o.id === d.offer_id);
  if (lost && (e.type === "offer.missed" || e.type === "offer.rejected"))
    notices = [...notices, { id: e.id, missed: e.type === "offer.missed", channel: lost.channel, from: lost.from, strike: d.strike }];
  if (ENDS_OFFER.has(e.type)) offers = offers.filter((o) => o.id !== d.offer_id);
  const lines = s.lines.map((line) => {
    if (line.cid !== e.conversation_id) return line;
    const t = line.transfer;
    const held = (e.type === "call.held" || e.type === "call.resumed") && d.participant_id == null ? e.type === "call.held" : line.held;
    const transfer = e.type === "call.transferred" && t && t.id === d.transfer_id ? (ENDS_TRANSFER.has(d.status) ? null : { ...t, status: String(d.status) }) : t;
    return { ...line, held, transfer, ...LINE_FLAGS[e.type] };
  });
  if (e.type === "agent.status_changed" && namesMe(s, e)) me = { ...me, status: d.to_status };
  if (onDial(s, e) && ENDS_DIAL.has(e.type)) dialing = null;
  const feed = namesMe(s, e) || onLine(s, e) || onDial(s, e) ? [e, ...s.feed.slice(0, 99)] : s.feed;
  return { ...s, offers, lines, me, dialing, feed, notices };
}

type Stint = Assignment & { via?: string | null };

function toLine(s: State, cid: string, conv: Conversation | null): Line | null {
  const a = conv?.assignments.filter((x) => x.agent_id === s.me.id).at(-1) as Stint | undefined;
  if (!conv || !a) return null;
  const was = s.lines.find((l) => l.cid === cid);
  return {
    held: false,
    muted: false,
    paused: false,
    transfer: null,
    ...was,
    stale: false,
    via: a.via ?? null,
    consult: a.role === "secondary" && a.via === "transfer" && a.mode !== "cold",
    cid,
    conv,
    phase: a.state === "wrap_up" ? "wrap" : "live",
    monitor: a.role === "monitor" ? (a.mode ?? "listen") : null,
    started: Date.parse(a.started_at),
    until: a.wrap_up_until ? Date.parse(a.wrap_up_until) : null,
  };
}

export function reduce(s: State, a: Action): State {
  switch (a.type) {
    case "phone":
      return { ...s, phone: { state: a.state, why: a.why ?? "" } };
    case "stream":
      return { ...s, stream: a.state };
    case "reachable":
      return { ...s, unreachable: !a.ok };
    case "renewed":
      return { ...s, me: a.me };
    case "event":
      return onEvent(s, a.e);
    case "ring":
      return { ...s, rings: [...s.rings, a.ring] };
    case "ring-gone":
      return { ...s, rings: s.rings.filter((r) => r.uuid !== a.uuid) };
    case "line": {
      const line = toLine(s, a.cid, a.conv);
      const was = s.lines.find((l) => l.cid === a.cid);
      if (!line && !was) return s;
      const names = a.conv?.queue_id && a.conv.queue_name ? { ...s.names, [a.conv.queue_id]: a.conv.queue_name } : s.names;
      const offers = line ? s.offers.filter((o) => o.cid !== a.cid) : s.offers;
      const lines = !line ? s.lines.filter((l) => l !== was) : was ? s.lines.map((l) => (l === was ? line : l)) : [...s.lines, line];
      const focus = line && line.phase !== was?.phase ? `${a.cid}/${line.phase}` : s.focus;
      return { ...s, lines, names, offers, focus };
    }
    case "dialing":
      return { ...s, dialing: a.dial };
    case "patch":
      return { ...s, lines: s.lines.map((l) => (l.cid === a.cid ? { ...l, ...a.patch } : l)) };
    case "names":
      return { ...s, names: { ...a.names, ...s.names } };
    case "status":
      return { ...s, me: { ...s.me, status: a.status } };
    case "statuses":
      return { ...s, statuses: a.statuses };
    case "dismiss":
      return { ...s, notices: s.notices.filter((n) => n.id !== a.id) };
    case "decline":
      return { ...s, offers: s.offers.filter((o) => o.id !== a.offer), rings: s.rings.filter((r) => r.uuid !== a.uuid) };
  }
}

export const autoWrap = (s: State, line: Line) => {
  const started = s.feed.find((e) => e.type === "conversation.wrap_up_started" && e.conversation_id === line.cid);
  // Only the event knows the mode: HS makes some wrap-ups manual whatever the setting, and a reload loses the event.
  return line.conv.channel === "call" && started?.data.mode === "auto";
};

export const stalled = (last: number, now: number) => now - last >= 35e3;

export function frames(text: string) {
  const blocks = text.split(/\r?\n\r?\n/);
  const rest = blocks.pop()!;
  const events = blocks.flatMap((b) => {
    const lines = b.split(/\r?\n/);
    const field = (name: string) => lines.filter((l) => l.startsWith(`${name}:`)).map((l) => l.slice(name.length + 1).replace(/^ /, ""));
    const data = field("data");
    return data.length ? [{ id: field("id").at(-1) ?? "", data: data.join("\n") }] : [];
  });
  return { events, rest };
}

export const voice = (s: State) => s.lines.find((l) => (l.conv.channel === "call" || l.via === "outbound") && l.phase === "live");

export const sdkFree = (s: State) => !s.rings.length && !s.offers.some((o) => o.channel === "call") && !s.dialing && !voice(s);

export const autoAnswer = (r: Ring) => r.kind === "outbound" || r.kind === "monitor" || r.autoanswer != null;

export type Pending = Offer & { ring: Ring | null };

export function pending(s: State): Pending[] {
  const held = (cid?: string) => s.lines.some((l) => l.cid === cid);
  const rings = s.rings.filter((r) => !autoAnswer(r) && !held(r.conversation));
  const out: Pending[] = s.offers.filter((o) => !held(o.cid)).map((o) => ({ ...o, ring: rings.find((r) => r.offer === o.id) ?? null }));
  for (const r of rings)
    if (!out.some((o) => o.ring === r))
      out.push({ id: r.offer ?? r.uuid, cid: r.conversation ?? "", channel: "call", from: r.from, to: null, queueId: null, via: r.kind === "transfer" ? "transfer" : "queue", ringFor: null, at: r.at, ring: r });
  return out;
}

export type Stage = { kind: "idle" } | { kind: "dialing"; dial: Dial } | { kind: "live" | "monitor"; line: Line };

export function stage(s: State): Stage {
  const L = voice(s);
  if (L) return { kind: L.monitor ? "monitor" : "live", line: L };
  return s.dialing ? { kind: "dialing", dial: s.dialing } : { kind: "idle" };
}

export const thread = (msgs: Message[], feed: Envelope[], say: (e: Envelope) => string) =>
  [
    ...feed.filter((e) => !e.type.startsWith("message.")).map((e) => ({ id: e.id, at: e.created_at, text: say(e) })),
    ...msgs.map((m) => ({ id: m.id, at: m.created_at, text: m.text ?? m.attachments.map((f) => f.name ?? "A file").join(", "), side: m.direction, note: m.direction === "outbound" ? m.status : undefined })),
  ].sort((a, b) => Date.parse(a.at) - Date.parse(b.at));

export const windowShut = (channel: string, msgs: Message[], now: number) =>
  channel === "whatsapp" && !msgs.some((m) => m.direction === "inbound" && now - Date.parse(m.created_at) < 24 * 3600e3);

export const replySubject = (msgs: Message[]) => {
  const was = msgs.findLast((m) => m.direction === "inbound" && m.email?.subject)?.email?.subject;
  return was && (/^re:/i.test(was) ? was : `Re: ${was}`);
};
