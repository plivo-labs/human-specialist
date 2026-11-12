import { Phone, RefreshCcw, User } from "lucide-react";
import { type Page, asAgent } from "../api";
import { kindIcon, kindWord } from "../conversations";
import { cap, clock, phone, plain, when } from "../format";
import type { ConversationSummary } from "../types";
import { Empty, Facts, Notice, Section, Tile, Timeline, cls, useLoad } from "../ui";
import { describe } from "../words";
import type { Ctx } from "./desk";
import { type Line, type Pending, TASK, TEXT, pending } from "./state";
import { Thread, Work } from "./thread";
import { WrapUp } from "./wrapup";

export const titled = (names: Record<string, string>, c: { id: string; channel: string; number: string | null }) => (TASK.has(c.channel) ? (names[c.id] ?? "Task") : phone(c.number));
const lineWord = (L: Line) => (L.monitor ? "Monitoring" : L.phase === "wrap" ? "Wrap-up" : L.held ? "On hold" : kindWord(L.conv));
export const left = (o: Pending, now: number) => (o.ringFor ? Math.max(0, o.ringFor - (now - o.at) / 1000) : null);
const iconButton = "grid size-7 place-items-center rounded-sm hover:bg-muted disabled:opacity-50";

type Pane = { ctx: Ctx; L: Line | null; open: boolean };

export function DeskPage({ ctx, id }: { ctx: Ctx; id?: string }) {
  const L = ctx.s.lines.find((l) => l.cid === id) ?? ctx.s.lines.at(-1) ?? null;
  const open = !!id || !!ctx.s.dialing;
  return (
    <div className="flex h-full">
      <Lines ctx={ctx} L={L} open={open} />
      <Center ctx={ctx} L={L} open={open} />
      <Details L={L} />
    </div>
  );
}

function Lines({ ctx, L, open }: Pane) {
  const { s, now } = ctx;
  const offers = pending(s);
  return (
    <aside aria-label="My Conversations" className={cls("w-full shrink-0 flex-col border-r border-st2 py-4 md:flex md:w-[315px]", open ? "hidden" : "flex")}>
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
            href={`#/desk/${l.cid}`}
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
      <div className={cls("min-w-0 flex-1 flex-col items-center justify-center", open ? "flex" : "hidden md:flex")}>
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
    <section aria-label="Conversation" className={cls("min-w-0 flex-1 flex-col overflow-y-auto px-4 pb-6", open ? "flex" : "hidden md:flex")}>
      <h2 className="subtitle py-3.5">{titled(s.names, L.conv)}</h2>
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
        <Timeline start rows={s.feed.filter((e) => e.conversation_id === L.cid).reverse().map((e) => ({ id: e.id, at: e.created_at, text: describe(e, name, s.me.id) }))} />
      )}
    </section>
  );
}

function Details({ L }: { L: Line | null }) {
  const number = L?.conv.number ?? null;
  const history = useLoad(
    () => (number ? asAgent<Page<ConversationSummary>>("GET", `/api/v1/conversations?number=${encodeURIComponent(number)}&sort=last_activity_at&limit=6`) : Promise.resolve(null)),
    [number],
  );
  const context = Object.entries(L?.conv.context ?? {});
  const past = number ? history.data?.data.filter((c) => c.id !== L?.cid).slice(0, 5) : [];
  if (!L) return null;
  return (
    <aside aria-label="Details" className="hidden w-[315px] shrink-0 overflow-y-auto border-l border-st2 xl:block">
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
    </aside>
  );
}
