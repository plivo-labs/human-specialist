import { CircleHelp, Plus } from "lucide-react";
import { useState } from "react";
import { type Page as Paged, api, every } from "../api";
import { phone } from "../format";
import type { Action, BusinessHours, EntryPoint, Named, Queue, Skill } from "../types";
import { Badge, Button, Confirm, Copy, Editor, Empty, Field, Input, ListView, Notice, Page, PhoneInput, Rows, Select, State, act, cls, useLoad } from "../ui";
import { NumbersFailed, allAgents, allQueues, names, put, useAccountNumbers } from "./agents";
import { OTHER_CHANNELS, type Picks, pickable, reach, route, routedBy, saveQueue, shownOf, textWait } from "./numbers";
import { Unit } from "./settings";

const off = (q: Queue) => (q.enabled ? q.name : `${q.name} (disabled)`);

function Picker({ label, name, rows, chosen, none, hint }: { label: string; name: string; rows: Named[]; chosen?: string[]; none: string; hint?: string }) {
  if (!rows.length) return <p className="text-s-400 text-secondary">{none}</p>;
  return (
    <fieldset className="grid max-h-64 gap-2 overflow-y-auto">
      <legend className="pb-1.5 text-s-500 text-secondary">{label}</legend>
      {hint && <p className="text-s-400 text-muted-foreground">{hint}</p>}
      {rows.map((a) => (
        <label key={a.id} className="flex items-center gap-2 text-p-400">
          <input type="checkbox" name={name} value={a.id} defaultChecked={chosen?.includes(a.id)} className="size-4 accent-primary" /> {a.name}
        </label>
      ))}
    </fieldset>
  );
}

const RULES = [["", "Any level"], [">=", "At least"], [">", "Above"], ["=", "Exactly"], ["!=", "Not"], ["<=", "At most"], ["<", "Below"]];

const Count = ({ ids, of, word }: { ids: string[]; of: { id: string; name: string | null }[]; word: string }) =>
  ids.length > 1 ? <Badge>{`${ids.length} ${word}s`}</Badge> : ids.length ? <>{names(ids, of)}</> : <span className="text-tertiary">No {word}s</span>;

export function Queues() {
  const load = useLoad(() =>
    Promise.all([
      allQueues(),
      allAgents(),
      every<EntryPoint>((c) => api<Paged<EntryPoint>>("GET", `/api/v1/numbers?limit=200${c}`)),
      every<Named>((c) => api<Paged<Named>>("GET", `/api/v1/menus?limit=200${c}`)),
      every<Named>((c) => api<Paged<Named>>("GET", `/api/v1/teams?limit=200${c}`)),
      every<Skill>((c) => api<Paged<Skill>>("GET", `/api/v1/skills?limit=200${c}`)),
    ]),
  );
  const [edit, setEdit] = useState<Queue | "new" | null>(null);
  const [queues, agents, numbers, menus, teams, skills] = load.data ?? [];
  const queue = edit === "new" ? undefined : (edit ?? undefined);
  const users = queue ? routedBy(queue.id, [...numbers!.map((n): [string, EntryPoint] => [phone(n.number), n]), ...menus!.map((m): [string, Named] => [`menu ${m.name}`, m]), ...queues!.map((q): [string, Queue] => [`queue ${q.name}`, q])]) : [];
  const flip = async () => (await act(queue!.enabled ? "Disabling the queue" : "Enabling the queue", () => api("PATCH", `/api/v1/queues/${queue!.id}`, { enabled: !queue!.enabled }), queue!.enabled ? "Queue disabled" : "Queue enabled")) && (load.reload(), setEdit(null));
  return (
    <Page title="Queues">
      <ListView
        load={{ ...load, data: queues }}
        search={(q) => q.name}
        open={setEdit}
        action={
          <Button variant="primary" onClick={() => setEdit("new")}>
            <Plus aria-hidden /> New queue
          </Button>
        }
        empty={<Empty title="No queues yet. Add a queue and choose the agents who answer it." />}
        cols={[
          ["Queue name", (q) => q.name],
          [
            <span key="p" className="inline-flex items-center" title="1 - Highest, 10 - Least">
              Priority <CircleHelp className="ml-1 size-3.5" aria-label="1 is the highest priority, 10 the least" />
            </span>,
            (q) => <span className={cls(q.priority < 5 ? "text-destructive" : "text-secondary")}><State tone={q.priority < 5 ? "bad" : "off"}>{q.priority}</State></span>,
          ],
          ["State", (q) => <State tone={q.enabled ? "live" : "off"}>{q.enabled ? "Active" : "Disabled"}</State>],
          ["Capacity", (q) => <span className="font-mono text-xs-400">{q.capacity}</span>],
          ["Agents", (q) => (q.teams?.length || q.skills?.length ? <Badge>{reach(q)}</Badge> : <Count ids={q.agents ?? []} of={agents!} word="agent" />)],
        ]}
      />
      {edit && (
        <Editor
          what="queue"
          row={queue}
          reload={load.reload}
          onClose={() => setEdit(null)}
          body={(f) => f}
          save={(f) => saveQueue(api, f, queue)}
          remove={queue && (() => api("DELETE", `/api/v1/queues/${queue.id}`))}
          left={
            queue &&
            (queue.enabled && users.length ? (
              <Confirm label="Disable queue" sure="Disable it anyway" run={flip} />
            ) : (
              <Button variant="danger" size="md" onClick={flip}>
                {queue.enabled ? "Disable queue" : "Enable queue"}
              </Button>
            ))
          }
        >
          <Field label="Queue name">
            <Input name="name" required autoFocus placeholder="Billing" defaultValue={queue?.name ?? ""} />
          </Field>
          <fieldset className="grid gap-1.5">
            <legend className="pb-0.5 text-s-500 text-secondary">Queue priority</legend>
            <div className="grid grid-cols-10 border">
              {Array.from({ length: 10 }, (_, i) => i + 1).map((n) => (
                <label key={n} className="grid h-[30px] place-items-center border-l font-mono text-[11px] first:border-l-0 has-[:checked]:bg-primary has-[:checked]:text-primary-foreground has-[:focus-visible]:outline-2 has-[:focus-visible]:outline-ring">
                  <input type="radio" name="priority" value={n} defaultChecked={n === (queue?.priority ?? 5)} className="sr-only" />
                  {n}
                </label>
              ))}
            </div>
            <span className="flex justify-between text-s-400 text-tertiary">
              <span>Highest</span>
              <span>Lowest</span>
            </span>
          </fieldset>
          <Field label="Capacity">
            <Input name="capacity" type="number" required min={1} defaultValue={queue?.capacity ?? 50} className="font-mono" />
          </Field>
          <fieldset className="grid gap-3">
            <legend className="pb-1.5 text-s-500 text-secondary">How long a contact waits before the queue gives up</legend>
            <div className="grid grid-cols-2 gap-4">
              <Unit label="Call" name="wait_call" value={queue?.wait_timeout_sec ?? 20} unit="seconds" min={0} max={86_400} />
              {OTHER_CHANNELS.map(([ch, label]) => (
                <Unit key={ch} label={label} name={`wait_${ch}`} value={textWait(queue, ch)} unit="seconds" min={0} max={604_800} />
              ))}
            </div>
            <p className="text-s-400 text-muted-foreground">A call waits 20 seconds unless you change it. 0 sets no time limit: a call or an email then waits for an agent, and any other contact leaves once every agent who could take it was offered it.</p>
          </fieldset>
          {users.length > 0 && <Notice>{queue!.enabled ? "Disabling it fails callers sent here by" : "Callers are sent here by"} {users.join(", ")}.</Notice>}
          <Picker label="Agents" name="agents" rows={agents ?? []} chosen={queue?.agents} none="No users yet. Add them under Users first." />
          <Picker label="Teams" name="teams" rows={teams ?? []} chosen={queue?.teams} none="No teams yet. Add them under Teams." hint="The queue offers its conversations to the agents checked above and to every member of the teams checked here." />
          {skills?.length ? (
            <div className="grid gap-1.5">
              <Rows label="Skills" add="Add skill" items={queue?.skills ?? []} blank={{ skill_id: "", operator: "", level: 0 }}>
                {(r) => (
                  <span className="grid flex-1 grid-cols-2 gap-2 sm:grid-cols-[1fr_8rem_5rem]">
                    <Select name="skills" required defaultValue={r.skill_id} aria-label="Skill" className="col-span-2 sm:col-span-1">
                      <option value="" disabled>
                        Choose a skill
                      </option>
                      {r.skill_id && !skills.some((s) => s.id === r.skill_id) && <option value={r.skill_id}>An unknown skill</option>}
                      {skills.map((s) => (
                        <option key={s.id} value={s.id}>
                          {s.name}
                        </option>
                      ))}
                    </Select>
                    <Select name="operator" defaultValue={r.operator} aria-label="Rule" className="[&:has(option[value='']:checked)+input]:invisible" onChange={(e) => e.target.value || ((e.target.parentElement!.nextElementSibling as HTMLInputElement).value = "")}>
                      {RULES.map(([v, l]) => (
                        <option key={v} value={v}>
                          {l}
                        </option>
                      ))}
                      {r.operator === "<>" && <option value="<>">Not</option>}
                    </Select>
                    <Input name="level" type="number" min={0} max={100} placeholder="0" defaultValue={r.level} aria-label="Level" className="font-mono" />
                  </span>
                )}
              </Rows>
              <p className="text-s-400 text-muted-foreground">With skills, only the agents and team members who hold at least one of these skills, at a level its rule admits, are offered the queue's conversations. A conversation's sticky agent among them is offered it whatever its skills. Any level admits whoever holds the skill, and is the only rule a skill without levels meets.</p>
            </div>
          ) : (
            <p className="text-s-400 text-secondary">No skills yet. Add them under Skills to route by skill.</p>
          )}
          {queue && (
            <Field label="Enqueue URL" hint={`Strategy: ${STRATEGY[queue.strategy ?? "availability"]}.`}>
              <span className="flex items-center gap-2">
                <Input readOnly value={queue.enqueue_url} onFocus={(e) => e.target.select()} />
                <Copy value={queue.enqueue_url} />
              </span>
            </Field>
          )}
        </Editor>
      )}
    </Page>
  );
}

const STRATEGY: Record<NonNullable<Queue["strategy"]>, string> = { availability: "Availability", simultaneous: "Simultaneous", fewest_handled: "Lowest Conversations Handled", least_handle_time: "Lowest Conversation Handle Time" };
const ROUTE: Record<string, string> = { menu: "A menu", voicemail: "Voicemail", forward: "Forwarded", redirect: "Your own XML", hangup: "Hang up" };

const kept = (a: Action | null | undefined, offered: string[]) => a && !offered.includes(a.type) && <option value={a.type}>{ROUTE[a.type] ?? a.type}</option>;

export function Numbers() {
  const load = useLoad(() =>
    Promise.all([
      every<EntryPoint>((c) => api<Paged<EntryPoint>>("GET", `/api/v1/numbers?limit=200${c}`)),
      allQueues(),
      every<BusinessHours>((c) => api<Paged<BusinessHours>>("GET", `/api/v1/business-hours?limit=200${c}`)),
    ]),
  );
  const owned = useAccountNumbers();
  const [edit, setEdit] = useState<EntryPoint | "new" | null>(null);
  const [numbers, queues, hours] = load.data ?? [];
  const reroute = (n: EntryPoint, queue_id: string) =>
    act(`Routing ${phone(n.number)}`, () => put(n.number, route({ ...shownOf(n), open: `queue:${queue_id}` }, shownOf(n), n)), `${phone(n.number)} now rings ${queues?.find((q) => q.id === queue_id)?.name}`);
  return (
    <Page title="Phone Numbers" sub="The numbers callers dial, the queue each one rings, and when it is open">
      <ListView
        load={{ ...load, data: numbers }}
        search={(n) => n.number}
        open={setEdit}
        action={
          <Button variant="primary" onClick={() => setEdit("new")} disabled={!queues?.length}>
            <Plus aria-hidden /> Add number
          </Button>
        }
        empty={<Empty title={queues?.length ? "No numbers yet. Add a number from your Plivo account and choose the queue its callers wait in." : "Add a queue first, then a number whose callers wait in it."} />}
        cols={[
          ["Number", (n) => <span className="font-mono font-normal">{phone(n.number)}</span>, "w-1/3"],
          [
            "Callers go to",
            (n) =>
              n.open_action.type === "queue" ? (
                <span onClick={(e) => e.stopPropagation()} onKeyDown={(e) => e.stopPropagation()}>
                  <Select
                    defaultValue={n.open_action.queue_id}
                    aria-label={`Queue for ${phone(n.number)}`}
                    className="max-w-72"
                    onChange={(e) => {
                      const el = e.target;
                      reroute(n, el.value).then((ok) => (ok ? load.reload() : (el.value = n.open_action.queue_id ?? "")));
                    }}
                  >
                    {pickable(queues!, n.open_action.queue_id ?? "").map((q) => (
                      <option key={q.id} value={q.id}>
                        {off(q)}
                      </option>
                    ))}
                  </Select>
                </span>
              ) : (
                (ROUTE[n.open_action.type] ?? n.open_action.type)
              ),
          ],
          ["Business hours", (n) => (n.business_hours_id ? (hours!.find((h) => h.id === n.business_hours_id)?.name ?? "Unknown") : "Always open"), "hidden sm:table-cell"],
        ]}
      />
      {edit && <NumberSheet row={edit === "new" ? undefined : edit} numbers={numbers!} owned={owned} queues={queues!} hours={hours!} reload={() => (load.reload(), owned.reload())} onClose={() => setEdit(null)} />}
    </Page>
  );
}

function NumberSheet({ row, numbers, owned, queues, hours, reload, onClose }: { row?: EntryPoint; numbers: EntryPoint[]; owned: ReturnType<typeof useAccountNumbers>; queues: Queue[]; hours: BusinessHours[]; reload: () => void; onClose: () => void }) {
  const [timed, setTimed] = useState(!!row?.business_hours_id);
  const shown = shownOf(row);
  const [opens, setOpens] = useState(shown.open);
  const where = (a: Action) => (a.type === "queue" ? (queues.find((q) => q.id === a.queue_id)?.name ?? "a queue") : (ROUTE[a.type] ?? a.type));
  const queueOptions = (label: (q: string) => string, chosen: string) =>
    pickable(queues, chosen).map((q) => (
      <option key={q.id} value={`queue:${q.id}`}>
        {label(off(q))}
      </option>
    ));
  return (
    <Editor
      what="number"
      row={row}
      reload={reload}
      onClose={onClose}
      body={(f) => {
        const number = row?.number ?? String(f.get("number")).trim();
        const old = numbers.find((n) => n.number === number);
        const got = (name: keyof Picks) => String(f.get(name) ?? shown[name]);
        return { number, old, put: route({ open: got("open"), hours: got("hours"), closed: got("closed") }, shown, old) };
      }}
      save={(b) => put(b.number, b.put)}
      remove={row && (() => api("DELETE", `/api/v1/numbers/${encodeURIComponent(row.number)}`))}
      warn={(b) => (!row && b.old ? [`${phone(b.number)} is already on the list, and its callers go to ${where(b.old.open_action)}. Move number changes only what you chose here.`, "Move number"] : undefined)}
    >
      {!row && (
        <>
          <Field label="Number" hint={owned.error ? <NumbersFailed owned={owned} /> : "Pick a number on your Plivo account, or type one with its country code."}>
            <PhoneInput name="number" required autoFocus list="account-numbers" />
          </Field>
          <datalist id="account-numbers">
            {[...(owned.data ?? [])].sort((a, b) => +a.routed - +b.routed).map((n) => (
              <option key={n.number} value={n.number}>
                {n.routed ? "Routed here" : "Not routed yet"}
              </option>
            ))}
          </datalist>
        </>
      )}
      <Field label="Callers go to">
        <Select name="open" required defaultValue={shown.open} onChange={(e) => setOpens(e.target.value)}>
          <option value="" disabled>
            Choose a queue
          </option>
          {kept(row?.open_action, ["queue"])}
          {queueOptions((q) => q, shown.open)}
        </Select>
      </Field>
      <Field label="Business hours" hint={hours.length ? undefined : "Add business hours under Business Hours & Holidays to close outside them."}>
        <Select name="hours" defaultValue={shown.hours} onChange={(e) => setTimed(!!e.target.value)}>
          <option value="">Always open</option>
          {hours.map((h) => (
            <option key={h.id} value={h.id}>
              {h.name}
            </option>
          ))}
        </Select>
      </Field>
      {timed && (
        <Field label="When closed" hint="A voicemail goes to the queue callers go to when open, so it needs a queue there.">
          <Select name="closed" defaultValue={shown.closed}>
            <option value="hangup">Hang up</option>
            {opens.startsWith("queue:") && <option value="voicemail">Take a voicemail</option>}
            {kept(row?.closed_action, ["queue", "voicemail", "hangup"])}
            {queueOptions((q) => `Send to ${q}`, shown.closed)}
          </Select>
        </Field>
      )}
    </Editor>
  );
}
