import { ListTodo, Phone, Plus, RefreshCcw, User } from "lucide-react";
import { useState } from "react";
import { type Page, asAgent } from "../api";
import { CHANNEL, kindIcon, kindWord } from "../conversations";
import { cap, clock, phone, plain, when } from "../format";
import { PriorityField } from "../setup/lists";
import type { ConversationSummary, Task, TransferTargets } from "../types";
import { Button, Editor, Empty, Facts, Field, Handoff, Input, Listed, Notice, PhoneInput, Section, Select, Textarea, Tile, cls, useLoad } from "../ui";
import type { Ctx } from "./desk";
import { KINDS, type Kind, type Line, type Pending, TASK, TEXT, asks, createTask, pending } from "./state";
import { Call, Thread, Work } from "./thread";
import { WrapUp } from "./wrapup";

export const titled = (names: Record<string, string>, c: { id: string; channel: string; number: string | null }) => (TASK.has(c.channel) ? (names[c.id] ?? "Task") : phone(c.number));
const lineWord = (L: Line) => (L.monitor ? "Monitoring" : L.phase === "wrap" ? "Wrap-up" : L.held ? "On hold" : kindWord(L.conv));
export const left = (o: Pending, now: number) => (o.ringFor ? Math.max(0, o.ringFor - (now - o.at) / 1000) : null);
const iconButton = "grid size-7 place-items-center rounded-sm hover:bg-muted disabled:opacity-50";
const middle = "md:col-start-2 md:row-span-2 md:row-start-1";

type Pane = { ctx: Ctx; L: Line | null; open: boolean };

export function DeskPage({ ctx, id }: { ctx: Ctx; id?: string }) {
  const L = ctx.s.lines.find((l) => l.cid === id) ?? ctx.s.lines.at(-1) ?? null;
  const open = !!id || !!ctx.s.dialing;
  return (
    <div className="flex h-full md:contents">
      <Lines ctx={ctx} L={L} open={open} />
      <Center ctx={ctx} L={L} open={open} />
      <Details ctx={ctx} L={L} />
    </div>
  );
}

function Lines({ ctx, L, open }: Pane) {
  const { s, now } = ctx;
  const offers = pending(s);
  return (
    <aside aria-label="My Conversations" className={cls("w-full shrink-0 flex-col border-r border-st2 py-4 md:col-start-1 md:row-start-2 md:flex md:w-[315px]", open ? "hidden" : "flex")}>
      <div className="flex items-center justify-between px-4 pb-5">
        <h1 className="subtitle">My Conversations</h1>
        <button type="button" className={iconButton} aria-label="Refresh" onClick={ctx.loadAll}>
          <RefreshCcw className="size-4" aria-hidden />
        </button>
      </div>
      <div className="grid content-start gap-1 overflow-y-auto pl-4 pr-2">
        {offers.map((o) => {
          const t = left(o, now);
          return (
            <Tile
              key={o.id}
              active
              title={titled(s.names, { id: o.cid, channel: o.channel, number: o.from })}
              when={t == null ? undefined : clock(t)}
              icon={kindIcon(o)}
              line={o.via === "transfer" ? "Transfer ringing" : `${kindWord({ ...o, direction: "inbound" })}${s.names[o.queueId ?? ""] ? `, ${s.names[o.queueId!]}` : ""}`}
            />
          );
        })}
        {s.dialing && !s.lines.some((l) => l.cid === s.dialing!.cid) && <Tile active title={phone(s.dialing.to)} icon={<Phone aria-hidden />} line="Calling" />}
        {s.lines.map((l) => (
          <Tile
            key={l.cid}
            href={`/desk/${l.cid}`}
            active={l === L}
            title={titled(s.names, l.conv)}
            when={l.phase === "wrap" ? (l.until ? clock((l.until - now) / 1000) : undefined) : clock((now - l.started) / 1000)}
            icon={kindIcon(l.conv)}
            line={`${lineWord(l)}${l !== L && l.conv.unread_count ? `, ${l.conv.unread_count} new` : ""}`}
          />
        ))}
      </div>
    </aside>
  );
}

function Center({ ctx, L, open }: Pane) {
  const { s } = ctx;
  const name = (id?: string | null, fallback = "an agent") => (id === s.me.id ? "you" : (s.names[id ?? ""] ?? fallback));
  if (!L) {
    const status = s.statuses.find((x) => x.id === s.me.status?.id);
    return (
      <div className={cls("min-w-0 flex-1 flex-col items-center justify-center", middle, open ? "flex" : "hidden md:flex")}>
        <Empty title="No active conversations to show">
          {!status?.availability
            ? `You are ${s.me.status?.name ?? "offline"}, so calls and texts do not reach you. Change your status at the top to take calls and texts.`
            : s.phone.state === "failed"
              ? "This browser's phone is not ready. The notice above says why."
              : "Calls ring in this browser, and texts, tasks and callbacks show here, while your status takes them."}
        </Empty>
      </div>
    );
  }
  return (
    <section aria-label="Conversation" className={cls("min-w-0 flex-1 flex-col overflow-y-auto px-4 pb-6", middle, open ? "flex" : "hidden md:flex")}>
      <h2 className="subtitle py-3.5">{titled(s.names, L.conv)}</h2>
      <Handoff turns={L.conv.transcript} />
      {L.stale && (
        <Notice>
          The desk could not refresh this conversation, so it may be out of date.{" "}
          <button type="button" className="underline" onClick={() => ctx.loadLine(L.cid)}>
            Try again
          </button>
        </Notice>
      )}
      {L.phase === "wrap" && !L.monitor ? (
        <div className="grid flex-1 place-items-center">
          <WrapUp key={L.cid} ctx={ctx} line={L} />
        </div>
      ) : TEXT.has(L.conv.channel) ? (
        <Thread key={L.cid} ctx={ctx} line={L} name={name} />
      ) : TASK.has(L.conv.channel) ? (
        <Work key={L.cid} ctx={ctx} line={L} name={name} />
      ) : (
        <Call key={L.cid} ctx={ctx} line={L} name={name} />
      )}
    </section>
  );
}

function Details({ ctx, L }: { ctx: Ctx; L: Line | null }) {
  const number = L?.conv.number ?? null;
  const history = useLoad(
    () => (number ? asAgent<Page<ConversationSummary>>("GET", `/api/v1/conversations?number=${encodeURIComponent(number)}&sort=last_activity_at&limit=6`) : Promise.resolve(null)),
    [number],
  );
  const heard = ctx.s.feed.find((e) => e.conversation_id === L?.cid && e.type.startsWith("task."))?.id;
  const tasks = useLoad(() => (L ? asAgent<Page<Task>>("GET", `/api/v1/tasks?conversation_id=${L.cid}&limit=50`) : Promise.resolve(null)), [L?.cid, heard]);
  const [raise, setRaise] = useState<{ from: Line | null }>();
  const context = Object.entries(L?.conv.context ?? {});
  const past = number ? history.data?.data.filter((c) => c.id !== L?.cid).slice(0, 5) : [];
  return (
    <aside aria-label="Details" className="hidden w-[315px] shrink-0 overflow-y-auto border-l border-st2 md:col-start-3 md:row-span-2 md:row-start-1 xl:block">
      <Section title="Actions">
        <Button onClick={() => setRaise({ from: L })}>
          <Plus aria-hidden /> Create task
        </Button>
        {raise && <NewTask ctx={ctx} from={raise.from} onClose={() => setRaise(undefined)} />}
      </Section>
      {L && (
        <>
          <Section title="Contact details" icon={<User aria-hidden />}>
            <Facts
              rows={(
                [
                  [L.conv.channel === "email" ? "Email" : "Phone", number && phone(number)],
                  ["Queue", L.conv.queue_name],
                  [L.conv.channel !== "call" ? "Our address" : L.conv.direction === "outbound" ? "Called from" : "Called", L.conv.our_address && phone(L.conv.our_address)],
                  ["Conversation", <code key="i" className="font-mono">#{L.conv.conversation_number}</code>],
                ] as [string, React.ReactNode][]
              ).filter(([, v]) => v)}
            />
          </Section>
          {context.length > 0 && (
            <Section title="Handoff context">
              <Facts rows={context.map(([k, v]) => [cap(plain(k)), typeof v === "object" ? JSON.stringify(v) : String(v)])} />
            </Section>
          )}
          <Section title="Tasks" icon={<ListTodo aria-hidden />}>
            <Listed load={{ ...tasks, data: tasks.data?.data.filter((t) => t.conversation_id !== L.cid) }} empty={<p className="text-s-400 text-secondary">None raised from this conversation.</p>}>
              {(rows) => (
                <ul className="grid gap-2 text-s-400">
                  {rows.map((t) => (
                    <li key={t.id} className="grid">
                      <span>{t.title}</span>
                      <span className="text-xs-400 text-tertiary">{[cap(plain(t.type)), t.channel && CHANNEL[t.channel], `${t.priority} priority`, plain(t.status), t.due_at && `due ${when(t.due_at)}`].filter(Boolean).join(", ")}</span>
                    </li>
                  ))}
                </ul>
              )}
            </Listed>
          </Section>
          <Section title="Earlier conversations">
            {history.error ? (
              <p className="text-s-400 text-destructive">{history.error}</p>
            ) : !past ? (
              <p className="text-s-400 text-secondary">Loading…</p>
            ) : past.length ? (
              <ul className="grid gap-2 text-s-400">
                {past.map((c) => (
                  <li key={c.id} className="grid">
                    <span>
                      {kindWord(c)}
                      {c.queue_name && `, ${c.queue_name}`}
                    </span>
                    <span className="text-xs-400 text-tertiary">
                      {when(c.created_at)}, {c.state === "closed" ? "closed" : "open"}
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-s-400 text-secondary">None that you can see.</p>
            )}
          </Section>
        </>
      )}
    </aside>
  );
}

const REACH = ["call", "whatsapp", "sms", "email", "chat"];
const soon = () => new Date(Date.now() - new Date().getTimezoneOffset() * 60e3).toISOString().slice(0, 16);
const group = (label: string, rows: [string, string][] = []) =>
  rows.length > 0 && (
    <optgroup label={label}>
      {rows.map(([value, name]) => (
        <option key={value} value={value}>
          {name}
        </option>
      ))}
    </optgroup>
  );

function NewTask({ ctx, from, onClose }: { ctx: Ctx; from: Line | null; onClose: () => void }) {
  const [kind, setKind] = useState<Kind>("internal");
  const [origin, setOrigin] = useState(from);
  const who = useLoad(() => asAgent<TransferTargets>("GET", `/api/v1/transfer-targets?channel=task`));
  const need = asks(kind, !!origin);
  const me = `agent:${ctx.s.me.id}`;
  const at = (name: "scheduled_at" | "due_at", label: string) =>
    name in need && (
      <Field label={label}>
        <Input name={name} type="datetime-local" {...(name === "scheduled_at" && { required: true, min: soon() })} className="font-mono" />
      </Field>
    );
  return (
    <Editor what="task" reload={ctx.loadAll} onClose={onClose} body={(f) => Object.fromEntries(f) as Record<string, string>} save={(b) => createTask(asAgent, b, origin?.cid).catch((e) => (e?.code === "conversation_closed" && setOrigin(null), Promise.reject(e)))}>
      <p className="text-s-400 text-secondary">
        {origin ? `Raised from ${titled(ctx.s.names, origin.conv)}, conversation #${origin.conv.conversation_number}.` : from ? "Its conversation has closed, so Create task now raises it from none." : "Not raised from a conversation."}
      </p>
      <Field label="Task type">
        <Select name="type" value={kind} onChange={(e) => setKind(e.target.value as Kind)}>
          {KINDS.map((k) => (
            <option key={k} value={k}>
              {cap(plain(k))}
            </option>
          ))}
        </Select>
      </Field>
      {"channel" in need && (
        <Field label="Channel">
          <Select name="channel">
            {REACH.map((c) => (
              <option key={c} value={c}>
                {cap(CHANNEL[c])}
              </option>
            ))}
          </Select>
        </Field>
      )}
      {at("scheduled_at", "Schedule")}
      <Field label="Title">
        <Input name="title" required autoFocus />
      </Field>
      <Field label="Description">
        <Textarea name="description" />
      </Field>
      {"customer" in need && (
        <Field label="Customer's number" hint={need.customer ? undefined : "Empty takes this conversation's customer."}>
          <PhoneInput name="customer" required={need.customer} pattern="\+[1-9]\d{6,14}" />
        </Field>
      )}
      <Field label="Assign to" hint={who.error && `Only you: the agents and teams did not load. ${who.error}`}>
        <Select name="target" defaultValue={me}>
          <option value={me}>Me</option>
          {group("Agents", who.data?.agents.map((a) => [`agent:${a.id}`, `${a.name ?? "Unnamed"}${a.availability === "busy" ? ", no free slot now" : ""}`]))}
          {group("Teams", who.data?.teams.map((t) => [`team:${t.id}`, `${t.name ?? "Unnamed team"}, ${t.available_agents} free now`]))}
        </Select>
      </Field>
      <PriorityField />
      {at("due_at", "Due date")}
    </Editor>
  );
}
