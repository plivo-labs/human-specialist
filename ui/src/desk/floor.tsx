import { useEffect } from "react";
import { type Page as Paged, asAgent, every } from "../api";
import { clock } from "../format";
import { go } from "../shell";
import type { Agent, Floor as FloorData } from "../types";
import { Button, Empty, Listed, Page, State, Table, type Tone, act, cls, td, tr, useLoad } from "../ui";
import { type Ctx, useNow } from "./desk";

type Row = FloorData["agents"][number] & { name: string; free: boolean };
const ACTIVITY: Record<string, [string, Tone, number]> = {
  on_call: ["On a call", "live", 0],
  ringing: ["Ringing", "ring", 1],
  wrap_up: ["In wrap-up", "wrap", 2],
  free: ["Free for a call", "free", 3],
  away: ["Not taking calls", "off", 4],
};
const kind = (r: Row) => (r.activity === "idle" ? (r.free ? "free" : "away") : r.activity);
const MODES = [
  { mode: "listen", label: "Listen" },
  { mode: "whisper", label: "Coach" },
  { mode: "barge", label: "Barge-in" },
];

const Tile = ({ label, value }: { label: string; value: string | number }) => (
  <div className="rounded-sm border px-5 py-4">
    <p className="label-wide text-secondary">{label}</p>
    <p className="mt-2 font-mono text-[18px] leading-[26px]">{value}</p>
  </div>
);

export function Floor({ ctx }: { ctx: Ctx }) {
  const { s } = ctx;
  const tick = useNow(5000);
  const names = useLoad(() => every<Agent>((c) => asAgent<Paged<Agent>>("GET", `/api/v1/agents?limit=200${c}`)));
  const floor = useLoad(() => asAgent<FloorData>("GET", `/api/v1/floor`), [tick]);
  useEffect(() => {
    if (names.data) ctx.dispatch({ type: "names", names: Object.fromEntries(names.data.map((a) => [a.id, a.name])) });
  }, [names.data]);
  const free = new Set(s.statuses.filter((x) => x.availability).map((x) => x.id));
  const rows: Row[] | undefined = floor.data?.agents
    .map((a) => ({ ...a, name: s.names[a.agent_id] ?? "Unnamed agent", free: !!a.status && free.has(a.status.id) }))
    .sort((a, b) => ACTIVITY[kind(a)][2] - ACTIVITY[kind(b)][2] || a.name.localeCompare(b.name));
  const count = (k: string) => rows?.filter((r) => kind(r) === k).length ?? "-";
  const queues = floor.data?.queues ?? [];
  const waiting = queues.reduce((n, q) => n + q.waiting, 0);
  const oldest = Math.max(0, ...queues.map((q) => q.oldest_wait_sec));
  const modes = MODES.filter((m) => m.mode !== "barge" || s.me.grants.barge);
  return (
    <div className="h-full overflow-auto">
      <Page title="Floor" sub="Who is on a call, ringing or free right now. Updates every 5 seconds.">
        <div className="mt-6 grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6">
          <Tile label="Free" value={count("free")} />
          <Tile label="On a call" value={count("on_call")} />
          <Tile label="Ringing" value={count("ringing")} />
          <Tile label="Wrap-up" value={count("wrap_up")} />
          <Tile label="Waiting" value={floor.data ? waiting : "-"} />
          <Tile label="Longest wait" value={floor.data ? clock(oldest) : "-"} />
        </div>
        <div className="mt-6 overflow-x-auto pb-6">
          <Listed load={{ ...floor, data: rows }} empty={<Empty title="No agents on your floor. Agents in your teams show here." />}>
            {(list) => (
              <Table head={[["Agent"], ["Activity"], ["Status", "hidden sm:table-cell"], [s.me.grants.monitor ? "Monitor" : "", "text-right"]]}>
                {list.map((r) => {
                  const [word, tone] = ACTIVITY[kind(r)];
                  return (
                    <tr key={r.agent_id} className={tr}>
                      <td className={cls(td, "font-medium")}>{r.agent_id === s.me.id ? `${r.name} (you)` : r.name}</td>
                      <td className={td}>
                        <State tone={tone}>{word}</State>
                      </td>
                      <td className={cls(td, "hidden text-secondary sm:table-cell")}>{r.status?.name ?? "Offline"}</td>
                      <td className={cls(td, "text-right")}>
                        {s.me.grants.monitor && r.activity === "on_call" && r.conversation_id && r.agent_id !== s.me.id && (
                          <span className="inline-flex gap-2">
                            {modes.map(({ mode, label }) => (
                              <Button
                                key={mode}
                                size="xs"
                                aria-label={`${label} on ${r.name}'s call`}
                                onClick={async () => {
                                  const ok = await act(`Joining ${r.name}'s call`, () => asAgent("POST", `/api/v1/calls/${r.conversation_id}/monitor`, { mode }), "Your phone joins the call now");
                                  if (ok) go("/desk");
                                }}
                              >
                                {label}
                              </Button>
                            ))}
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </Table>
            )}
          </Listed>
        </div>
        {queues.length > 0 && <p className="pb-6 text-s-400 text-secondary">Waiting by queue: {queues.map((q) => `${q.name} ${q.waiting}`).join(", ")}.</p>}
      </Page>
    </div>
  );
}
