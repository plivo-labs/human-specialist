import type { Action, EntryPoint } from "../types";

export type Picks = { open: string; hours: string; closed: string };
export type Route = Omit<EntryPoint, "id" | "number">;

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

export const pickable = <Q extends { id: string; enabled: boolean }>(queues: Q[], chosen: string) => queues.filter((q) => q.enabled || `queue:${q.id}` === chosen || q.id === chosen);
