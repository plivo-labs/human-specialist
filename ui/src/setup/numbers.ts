import type { Action, Agent, EntryPoint, Queue } from "../types";

export type Picks = { open: string; hours: string; closed: string };
export type Route = Omit<EntryPoint, "id" | "number">;
type Call = <T>(method: string, path: string, body?: unknown) => Promise<T>;

export const OTHER_CHANNELS = [["chat", "Chat"], ["sms", "SMS"], ["whatsapp", "WhatsApp"], ["email", "Email"], ["callback", "Callback"], ["task", "Task"]];
export const textWait = (q: Pick<Queue, "text"> | undefined, ch: string) => (typeof q?.text?.wait_timeout_sec === "number" ? q.text.wait_timeout_sec : (q?.text?.wait_timeout_sec[ch] ?? 0));

const key = (v: unknown) => JSON.stringify(Array.isArray(v) ? v.map((x) => JSON.stringify(x)).sort() : v);
const changed = (b: object, was: object) => Object.fromEntries(Object.entries(b).filter(([k, v]) => key(v) !== key((was as Record<string, unknown>)[k])));

export async function saveQueue(call: Call, f: FormData, q?: Queue) {
  const waits = Object.fromEntries(OTHER_CHANNELS.map(([ch]) => [ch, Number(f.get(`wait_${ch}`))]));
  const skills = f.getAll("skills").map((id, i) => ({ skill_id: id, operator: f.getAll("operator")[i], level: Number(f.getAll("level")[i]) }));
  const body = { name: f.get("name"), priority: Number(f.get("priority")), capacity: Number(f.get("capacity")), agents: f.getAll("agents"), teams: f.getAll("teams"), skills, wait_timeout_sec: Number(f.get("wait_call")) };
  if (!q) return call("POST", `/api/v1/queues`, { ...body, text: { wait_timeout_sec: waits } }).catch(gone);
  const moved = Object.entries(waits).filter(([ch, w]) => w !== textWait(q, ch));
  const text = moved.length ? (await call<Queue>("GET", `/api/v1/queues/${q.id}`)).text : undefined;
  const merged = { ...text, wait_timeout_sec: { ...Object.fromEntries(OTHER_CHANNELS.map(([ch]) => [ch, textWait({ text }, ch)])), ...Object.fromEntries(moved) } };
  return call("PATCH", `/api/v1/queues/${q.id}`, { ...changed(body, q), ...(moved.length && { text: merged }) }).catch(gone);
}

export const reach = (q: Queue) => ([[q.agents, "agent"], [q.teams, "team"], [[...new Set(q.skills?.map((s) => s.skill_id))], "skill"]] as const).flatMap(([ids, word]) => (ids?.length ? [`${ids.length} ${word}${ids.length > 1 ? "s" : ""}`] : [])).join(", ");

const gone = (e: Refused) => {
  const kind = e?.code === "foreign_id" && ["agent", "team", "skill"].includes(e.details?.kind ?? "") ? e.details!.kind : undefined;
  if (!kind) throw e;
  throw Object.assign(new Error(`This ${kind} was deleted. ${kind === "skill" ? "Remove its row" : "Uncheck it"} to save.`), { field: `${kind}s=${e.details!.ids?.[0]}` });
};

export const userBody = (f: FormData) => ({
  name: f.get("name"),
  role: f.get("role"),
  email: String(f.get("email")).trim() || null,
  teams: f.getAll("teams"),
  skills: f.getAll("skills").map((id) => ({ skill_id: id, level: f.has(`level_${id}`) ? Number(f.get(`level_${id}`)) : null })),
  caller_id: String(f.get("caller_id")).trim() || null,
});
type Refused = { code?: string; details?: { skill_id?: string; kind?: string; ids?: string[] } };
export const sendUser = (call: Call, b: ReturnType<typeof userBody>, was?: Agent) =>
  (was ? call<Agent>("PATCH", `/api/v1/agents/${was.id}`, changed(b, was)) : call<Agent>("POST", `/api/v1/agents`, b)).catch((e: Refused) => {
    const deleted = e?.code === "foreign_id" && e.details?.kind === "skill" ? e.details.ids?.[0] : undefined;
    const id = deleted ?? (e?.code === "skill_level_invalid" ? e.details?.skill_id : undefined);
    if (!id) throw e;
    const level = !deleted && b.skills.find((s) => s.skill_id === id)?.level != null;
    throw Object.assign(new Error(deleted ? "This skill was deleted. Uncheck it to remove it." : "This skill's levels changed after the page loaded. Reload the page to see them."), { field: `${level ? "level" : "skill"}_${id}` });
  });

const keyOf = (a: Action | null | undefined, none: string) => (!a ? none : a.type === "queue" ? `queue:${a.queue_id}` : a.type);
const toAction = (k: string, was: Action | null | undefined, mailbox?: string): Action | null =>
  k === "hangup" ? null : k === "voicemail" ? { type: "voicemail", mailbox: { queue_id: mailbox } } : { ...(was?.type === "queue" ? was : { type: "queue" }), queue_id: k.slice(6) };

export const shownOf = (row?: EntryPoint): Picks => ({ open: keyOf(row?.open_action, ""), hours: row?.business_hours_id ?? "", closed: keyOf(row?.closed_action, "hangup") });

export function route(picked: Picks, shown: Picks, old?: EntryPoint): Route {
  const same = (k: keyof Picks) => !!old && picked[k] === shown[k];
  const open_action = same("open") ? old!.open_action : toAction(picked.open, old?.open_action)!;
  const business_hours_id = same("hours") ? old!.business_hours_id : picked.hours || null;
  return { open_action, business_hours_id, closed_action: same("closed") ? old!.closed_action : toAction(picked.closed, old?.closed_action, open_action.queue_id) };
}

export const routedBy = (id: string, from: [label: string, row: { id: string }][]) => from.filter(([, r]) => r.id !== id && JSON.stringify(r).includes(`"queue_id":"${id}"`)).map(([label]) => label);

export const callersTo = <Q extends { id: string; enabled: boolean; agents?: string[] }>(queues: Q[], agentId?: string) =>
  queues.find((q) => q.enabled && !!agentId && q.agents?.includes(agentId)) ?? queues.find((q) => q.enabled);

export const routeTo = (q?: { id: string }): Route => ({ open_action: q ? { type: "queue", queue_id: q.id } : { type: "hangup" }, business_hours_id: null, closed_action: null });

type Listed = { number: string; routed: boolean }[] | undefined;
export const fresh = (listed: Listed, routed: Set<string>, n: string | null) => !!n && !routed.has(n) && !!listed?.some((x) => x.number === n && !x.routed);

export async function saveUser(form: { listed: Listed; routed: Set<string>; users: { id: string; name: string | null }[] }, callerId: string | null, route: (n: string) => Promise<unknown>, save: () => Promise<unknown>) {
  const number = fresh(form.listed, form.routed, callerId) ? callerId : null;
  if (number) await route(number).then(() => form.routed.add(number));
  return save().catch((e) => {
    if (e?.code === "agent_exists") e = Object.assign(new Error(`${form.users.find((u) => u.id === e.details?.id)?.name ?? "Another user"} already signs in with this email. Each user needs their own.`), { field: "email" });
    if (number) e.message = `${number} now routes to this organization, but the user was not saved. ${e.message}`;
    throw e;
  });
}

export const pickable = <Q extends { id: string; enabled: boolean }>(queues: Q[], chosen: string) => queues.filter((q) => q.enabled || `queue:${q.id}` === chosen || q.id === chosen);
