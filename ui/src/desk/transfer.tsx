import * as RTabs from "@radix-ui/react-tabs";
import { RefreshCw, UserRoundPlus } from "lucide-react";
import { useState } from "react";
import { asAgent } from "../api";
import { phone } from "../format";
import type { TransferTargets } from "../types";
import { Button, Empty, Input, Listed, Tab, TabPanel, Tabs, act, cls, useLoad } from "../ui";
import type { Ctx } from "./desk";
import type { Line } from "./state";

type Target = { type: "agent"; agent_id: string } | { type: "team"; team_id: string } | { type: "destination"; destination_id: string } | { type: "number"; number: string };
type Row = { key: string; name: string; sub: string; target: Target | null; person?: boolean };

const ORDER = { available: 0, busy: 1, unavailable: 2 };
const initials = (name: string) =>
  name
    .split(/\s+/)
    .slice(0, 2)
    .map((w) => w[0]?.toUpperCase() ?? "")
    .join("");

function Rows({ rows, send }: { rows: Row[]; send: (mode: "warm" | "cold", t: Target, label: string) => Promise<unknown> }) {
  return (
    <ul className="grid">
      {rows.map((r) => (
        <li key={r.key} className={cls("flex items-center gap-2 overflow-hidden rounded-sm border-b border-st2 p-2 px-1 last:border-b-0 hover:bg-muted", !r.target && "opacity-60")}>
          {r.person && (
            <span aria-hidden className="grid size-7 shrink-0 place-items-center rounded-full bg-blue-200 text-s-400 text-blue-900">
              {initials(r.name)}
            </span>
          )}
          <span className="grid min-w-0 flex-1">
            <span className="truncate text-p-400">{r.name}</span>
            <span className="truncate text-s-400 text-tertiary">{r.sub}</span>
          </span>
          {r.target && (
            <span className="flex gap-2">
              <Button size="xs" variant="primary" aria-label={`Warm transfer to ${r.name}`} onClick={() => send("warm", r.target!, r.name)}>
                Warm
              </Button>
              <Button size="xs" aria-label={`Cold transfer to ${r.name}`} onClick={() => send("cold", r.target!, r.name)}>
                Cold
              </Button>
            </span>
          )}
        </li>
      ))}
    </ul>
  );
}

export function Transfer({ ctx, line, onDone }: { ctx: Ctx; line: Line; onDone: () => void }) {
  const { s, dispatch } = ctx;
  const load = useLoad(() => asAgent<TransferTargets>("GET", `/api/v1/transfer-targets?channel=call`));
  const [number, setNumber] = useState("");
  const t = load.data;
  const send = async (mode: "warm" | "cold", target: Target, label: string) => {
    const made = await act(`Transferring to ${label}`, () => asAgent<{ transfer_id: string }>("POST", `/api/v1/calls/${line.cid}/transfers`, { mode, target }), mode === "warm" ? `Ringing ${label}. Stay on the line.` : `Call handed to ${label}`);
    if (!made) return;
    if (mode === "warm") dispatch({ type: "patch", cid: line.cid, patch: { transfer: { id: made.transfer_id, mode, label, status: "initiated" } } });
    onDone();
  };
  const agents: Row[] = (t?.agents ?? [])
    .filter((a) => a.id !== s.me.id)
    .sort((a, b) => ORDER[a.availability] - ORDER[b.availability])
    .map((a) => ({
      key: a.id,
      name: a.name ?? "Unnamed",
      person: true,
      sub: a.availability === "available" ? (a.status?.name ?? "Available") : a.availability === "busy" ? "On another call" : `${a.status?.name ?? "Offline"}, not taking calls`,
      target: a.availability === "available" ? { type: "agent", agent_id: a.id } : null,
    }));
  const teams: Row[] = (t?.teams ?? [])
    .sort((a, b) => b.available_agents - a.available_agents)
    .map((x) => ({
      key: x.id,
      name: x.name ?? "Unnamed team",
      sub: x.available_agents ? `${x.available_agents} available` : "No one available",
      target: x.available_agents ? { type: "team", team_id: x.id } : null,
    }));
  const saved: Row[] = (t?.destinations ?? []).map((d) => ({ key: d.id, name: d.name ?? "Saved number", sub: "Internal contact", target: { type: "destination", destination_id: d.id } }));
  const list = (rows: Row[], empty: string) => (
    <Listed load={{ ...load, data: t && rows }} empty={<Empty title={empty} />}>
      {(r) => <Rows rows={r} send={send} />}
    </Listed>
  );
  const valid = /^\+\d{6,15}$/.test(number);
  return (
    <Tabs defaultValue="agent" className="mt-4 overflow-hidden">
      <RTabs.List aria-label="Transfer to" className="flex w-full items-center gap-4 border-b">
        <Tab value="agent">Agent</Tab>
        <Tab value="team">Team</Tab>
        <Tab value="contacts">Contacts</Tab>
        <button type="button" className="ml-auto grid size-8 place-items-center rounded-sm hover:bg-muted" aria-label="Refresh who is available" onClick={load.reload}>
          <RefreshCw className="size-4" aria-hidden />
        </button>
      </RTabs.List>
      <TabPanel value="agent" className="mt-4 max-h-64 overflow-y-auto">
        {list(agents, "No other agents")}
      </TabPanel>
      <TabPanel value="team" className="mt-4 max-h-64 overflow-y-auto">
        {list(teams, "No teams")}
      </TabPanel>
      <TabPanel value="contacts" className="mt-4 grid max-h-64 gap-3 overflow-y-auto">
        {s.me.settings.agent_direct_dial ? (
          <div className="flex items-center gap-2">
            <Input className="min-w-0 flex-1" value={number} onChange={(e) => setNumber(e.target.value.trim())} inputMode="tel" placeholder="+14155550100" aria-label="Phone number" />
            <Button size="xs" variant="primary" disabled={!valid} onClick={() => send("warm", { type: "number", number }, phone(number))}>
              Warm
            </Button>
            <Button size="xs" disabled={!valid} onClick={() => send("cold", { type: "number", number }, phone(number))}>
              Cold
            </Button>
          </div>
        ) : (
          <p className="text-s-400 text-secondary">Transfers to a phone number are off for your team. An admin can turn them on.</p>
        )}
        {saved.length > 0 && <Rows rows={saved} send={send} />}
      </TabPanel>
    </Tabs>
  );
}

export function TransferBar({ ctx, line }: { ctx: Ctx; line: Line }) {
  const t = line.transfer!;
  const joined = t.status === "accepted";
  const clear = () => ctx.dispatch({ type: "patch", cid: line.cid, patch: { transfer: null } });
  return (
    <div className="mt-2 flex flex-col border-t py-2" role="status">
      <p className="mb-2 mt-1 flex items-center gap-1 text-xs-400 text-secondary">
        <UserRoundPlus className="size-4 text-available" aria-hidden /> Warm transfer
      </p>
      <div className="ml-5 flex flex-col">
        <span className="text-p-600">{t.label}</span>
        <span className="text-p-400 text-tertiary">{joined ? "Connected. Talk to them, then complete the transfer." : "Ringing. The caller is on hold."}</span>
        <div className="mt-2 flex gap-2">
          <Button
            size="xs"
            variant="primary"
            disabled={!joined}
            title={joined ? undefined : `Complete it once ${t.label} answers`}
            onClick={() => act("Completing the transfer", () => asAgent("POST", `/api/v1/calls/${line.cid}/transfers/${t.id}/complete`), "Transfer complete. Wrap-up is open.")}
          >
            Complete transfer
          </Button>
          <Button
            size="xs"
            onClick={async () => {
              if (await act("Cancelling the transfer", () => asAgent("POST", `/api/v1/calls/${line.cid}/transfers/${t.id}/cancel`), "Transfer cancelled. You are back with the caller.")) clear();
            }}
          >
            Cancel
          </Button>
        </div>
      </div>
    </div>
  );
}
