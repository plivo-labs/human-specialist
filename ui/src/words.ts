import { cap, clock, phone, plain } from "./format";
import type { Envelope } from "./types";

type Name = (id: string | null | undefined, fallback?: string) => string;
type D = Record<string, any>;
const MODES: Record<string, string> = { listen: "listening", whisper: "coaching", barge: "barging in" };
const TRANSFER: Record<string, string> = { initiated: "started", accepted: "accepted", completed: "completed", failed: "failed", cancelled: "cancelled" };
function targetName(t: D | undefined, name: Name) {
  const id = t?.agent_id ?? t?.team_id ?? (t?.type === "agent" || t?.type === "team" ? t.id : null);
  if (id) return name(id, t!.type === "team" || t!.team_id ? "a team" : "an agent");
  const number = t?.number ?? (t?.type === "external" ? t.id : null);
  return number ? phone(number) : plain(t?.type ?? "a target");
}

const SAY: Record<string, (d: D, who: string, name: Name, me: boolean) => string> = {
  "conversation.started": (d) =>
    d.direction === "outbound" ? `Outbound ${d.channel === "call" ? "call" : d.channel} from ${phone(d.number)}` : `${d.channel === "call" ? "Call" : "Contact"} came in${d.number ? ` on ${phone(d.number)}` : ""}`,
  "conversation.queued": (d, _, name) => `Waiting in ${name(d.queue_id, "a queue")}`,
  "conversation.overflowed": (d) => `Left the queue: ${plain(d.reason)}`,
  "conversation.after_hours": () => "Arrived outside business hours",
  "conversation.menu_input": (d) => (d.input ? `Pressed ${d.input} in a menu` : "Pressed nothing in a menu"),
  "conversation.redirected": () => "Sent to another Plivo application",
  "offer.created": (d, who) => `Ringing ${who}${d.via === "transfer" ? " by transfer" : ""}`,
  "offer.accepted": (_, who) => `${cap(who)} accepted`,
  "offer.rejected": (_, who) => `${cap(who)} declined`,
  "offer.missed": (d, who) => `${cap(who)} missed the ring${d.strike ? `, strike ${d.strike}` : ""}`,
  "offer.withdrawn": (d, who) => `Ring to ${who} stopped${d.reason === "taken" ? ", someone else answered" : `: ${plain(d.reason)}`}`,
  "call.started": (d) => `Call from ${phone(d.from)} to ${phone(d.to)}`,
  "call.answered": (d, who) => (d.direction === "outbound" ? "The customer answered" : `${cap(who)} answered`),
  "call.failed": (d) => `Call failed: ${plain(d.result)}`,
  "call.held": (d, _, name) => `Call held${d.by_agent_id ? ` by ${name(d.by_agent_id)}` : ""}`,
  "call.resumed": (d, _, name) => `Call taken off hold${d.by_agent_id ? ` by ${name(d.by_agent_id)}` : ""}`,
  "call.recording_paused": () => "Recording paused",
  "call.recording_resumed": () => "Recording resumed",
  "call.transferred": (d, _, name) => `${d.mode === "warm" ? "Warm" : "Cold"} transfer to ${targetName(d.target, name)} ${TRANSFER[d.status] ?? plain(d.status)}`,
  "call.participant_added": (d, _, name) => `${cap(targetName(d.participant, name))} joined the call`,
  "call.participant_removed": (d, _, name) => `${cap(targetName(d.participant, name))} left the call: ${plain(d.reason)}`,
  "supervisor.joined": (d, _, name) => `${cap(name(d.supervisor_id, "A supervisor"))} started ${MODES[d.mode] ?? d.mode}`,
  "supervisor.mode_changed": (d, _, name) => `${cap(name(d.supervisor_id, "A supervisor"))} is now ${MODES[d.mode] ?? d.mode}`,
  "supervisor.left": (d, _, name) => `${cap(name(d.supervisor_id, "A supervisor"))} stopped monitoring`,
  "call.ended": (d) => `Call ended by ${plain(d.ended_by)}${d.talk_sec != null ? ` after ${clock(d.talk_sec)} of talk` : ""}`,
  "conversation.wrap_up_started": (_, who) => `${cap(who)} started wrap-up`,
  "conversation.wrapped_up": (d, who) => `${cap(who)} ${d.skipped ? "skipped wrap-up" : `wrapped up${d.disposition ? ` as ${d.disposition}` : ""}`}`,
  "conversation.closed": (d) => `Conversation closed: ${plain(d.result)}`,
  "recording.ready": () => "Recording ready",
  "voicemail.received": (d) => `Voicemail from ${phone(d.from)}`,
  "agent.status_changed": (d, who, _, me) =>
    `${me ? "Your" : `${cap(who)}'s`} status is now ${d.to_status?.name ?? "unknown"}${d.reason === "rona" ? " after missed rings" : ""}`,
};


export function describe(e: Envelope, name: Name, me?: string) {
  const self = !!me && e.agent_id === me;
  const who = self ? "you" : name(e.agent_id, "an agent");
  return SAY[e.type]?.({ direction: e.direction, ...e.data }, who, name, self) ?? cap(plain(e.type.replace(".", " ")));
}
