import { MessageSquare, Phone, PhoneIncoming, PhoneOutgoing, RefreshCcw, User } from "lucide-react";
import { useRef, useState } from "react";
import { type Page as Paged, api, every, reason } from "./api";
import { thread } from "./desk/state";
import { ago, cap, clock, dmy, phone, plain, time, when } from "./format";
import { allAgents, allQueues, names } from "./setup/agents";
import type { Conversation as Conv, ConversationSummary, Envelope, Message, Recording, Task } from "./types";
import { Empty, Facts, ListView, Listed, Page, Section, State, Tile, Timeline, type Tone, useLoad, usePaged } from "./ui";
import { describe } from "./words";

const STATE: Record<ConversationSummary["state"], [string, Tone]> = { active: ["Live", "live"], transferring: ["Transferring", "hold"], in_wrap_up: ["Wrap-up", "wrap"], closed: ["Closed", "off"] };
const stateOf = (c: Pick<ConversationSummary, "state" | "agent_ids">) => (c.state === "active" && !c.agent_ids.length ? (["Waiting", "free"] as [string, Tone]) : STATE[c.state]);
const length = (c: ConversationSummary) => (c.ended_at ? clock((Date.parse(c.ended_at) - Date.parse(c.created_at)) / 1000) : "Still open");
export const kindIcon = (c: Pick<ConversationSummary, "channel">) => (c.channel === "call" ? <Phone aria-hidden /> : <MessageSquare aria-hidden />);
export const CHANNEL: Record<string, string> = { call: "call", sms: "SMS", whatsapp: "WhatsApp", email: "email", chat: "chat", callback: "callback", task: "task" };
export const kindWord = (c: Pick<ConversationSummary, "channel" | "direction">) =>
  c.channel === "task" || c.channel === "callback" ? cap(c.channel) : `${c.direction === "inbound" ? "Incoming" : "Outgoing"} ${CHANNEL[c.channel] ?? plain(c.channel)}`;
const contact = (c: Pick<ConversationSummary, "id" | "channel" | "number">, titles: Record<string, string>) => (c.number || c.channel !== "task" ? phone(c.number) : (titles[c.id] ?? "Task"));

export function Conversations({ id }: { id?: string }) {
  const list = usePaged<ConversationSummary>((c) => api<Paged<ConversationSummary>>("GET", `/api/v1/conversations?limit=50${c}`));
  const agents = useLoad(allAgents);
  const tasks = useLoad(() => every<Task>((c) => api<Paged<Task>>("GET", `/api/v1/tasks?limit=200${c}`)));
  const titles = Object.fromEntries((tasks.data ?? []).map((t) => [t.conversation_id, t.title]));
  const who = (c: ConversationSummary) => names(c.agent_ids, agents.data ?? []) || "-";
  if (id)
    return (
      <div className="flex h-full">
        <div className="hidden w-[315px] shrink-0 flex-col border-r border-st2 py-4 lg:flex">
          <div className="flex items-center justify-between px-4 pb-5">
            <h1 className="subtitle">All Conversations</h1>
            <button type="button" className="rounded-sm p-1 text-tertiary hover:bg-muted" aria-label="Refresh" onClick={list.reload}>
              <RefreshCcw className="size-4" aria-hidden />
            </button>
          </div>
          <div className="grid content-start gap-1 overflow-y-auto pl-4 pr-2">
            {list.data?.map((c) => <Tile key={c.id} href={`#/conversations/${c.id}`} active={c.id === id} title={contact(c, titles)} when={ago(c.created_at)} icon={kindIcon(c)} line={kindWord(c)} />)}
          </div>
        </div>
        <Conversation id={id} titles={titles} />
      </div>
    );
  return (
    <Page title="All Conversations">
      <ListView
        load={list}
        more={list.more}
        search={(c) => `${c.number} ${c.conversation_number}`}
        placeholder="Search the loaded conversations"
        refresh
        open={(c) => (location.hash = `/conversations/${c.id}`)}
        empty={<Empty title="No conversations yet. Calls show here once a caller reaches a queue." />}
        cols={[
          ["Contact", (c) => contact(c, titles)],
          ["Created date", (c) => <span className="font-mono text-xs-400">{dmy(c.created_at)}</span>],
          ["Agent name", who, "hidden md:table-cell"],
          ["Queue", (c) => c.queue_name ?? "-", "hidden md:table-cell"],
          ["State", (c) => <State tone={stateOf(c)[1]}>{stateOf(c)[0]}</State>],
        ]}
      />
    </Page>
  );
}

function Conversation({ id, titles }: { id: string; titles: Record<string, string> }) {
  const load = useLoad(
    () =>
      Promise.all([
        api<Conv>("GET", `/api/v1/conversations/${id}`),
        api<Paged<Envelope>>("GET", `/api/v1/conversations/${id}/timeline?limit=200`),
        api<Paged<Recording>>("GET", `/api/v1/recordings?conversation_id=${id}`),
        allAgents(),
        allQueues(),
        every<Message>((cur) => api<Paged<Message>>("GET", `/api/v1/conversations/${id}/messages?limit=200${cur}`)),
      ]),
    [id],
  );
  const [c, timeline, recordings, agents, queues, messages] = load.data ?? [];
  const name = (aid?: string | null, fallback = "An agent") => [...agents!, ...queues!].find((a) => a.id === aid)?.name ?? fallback;
  return (
    <Listed load={{ ...load, data: load.data && [c!] }} empty={null}>
      {() => (
        <>
          <div className="min-w-0 flex-1 overflow-y-auto px-4 pb-6">
            <h2 className="subtitle py-3.5">{contact(c!, titles)}</h2>
            <Timeline start rows={thread(messages!, timeline!.data, (e) => describe(e, name))} />
            {recordings!.data.map((r) => (
              <section key={r.id} className="mt-6 grid max-w-[406px] gap-4 rounded-sm border p-4">
                <h3 className="flex items-center gap-2 text-p-400">
                  {c!.direction === "inbound" ? <PhoneIncoming aria-hidden /> : <PhoneOutgoing aria-hidden />}
                  <span className="flex-1">{kindWord(c!)} recording</span>
                  <span className="font-mono text-xs-400 text-secondary">{time(r.created_at)}</span>
                </h3>
                <Audio id={r.id} url={r.audio_url} />
                {r.duration_sec != null && <p className="font-mono text-xs-400 text-secondary">{clock(r.duration_sec)}</p>}
              </section>
            ))}
          </div>
          <aside aria-label="Details" className="hidden w-[315px] shrink-0 overflow-y-auto border-l border-st2 md:block">
            <Section title="Contact details" icon={<User aria-hidden />}>
              <Facts
                rows={[
                  ["Phone", phone(c!.number)],
                  ["Our number", c!.our_address ? phone(c!.our_address) : "Unknown"],
                ]}
              />
            </Section>
            <Section title="Conversation">
              <Facts
                rows={[
                  ["Conversation", <code key="n" className="font-mono">#{c!.conversation_number}</code>],
                  ["State", <State key="s" tone={stateOf(c!)[1]}>{stateOf(c!)[0]}</State>],
                  ["Queue", c!.queue_name ?? "None"],
                  ["Agents", names(c!.agent_ids, agents!) || "No agent"],
                  ["Started", when(c!.created_at)],
                  ["Length", length(c!)],
                ]}
              />
            </Section>
            {c!.dispositions.length > 0 && (
              <Section title="Wrap-up">
                <ul className="grid gap-1 text-s-400">
                  {c!.dispositions.map((d, i) => (
                    <li key={i}>
                      {name(d.agent_id, "Automatic")}: {d.skipped ? "skipped" : [d.disposition, d.sub_disposition].filter(Boolean).join(", ")}
                    </li>
                  ))}
                </ul>
              </Section>
            )}
            {c!.context && Object.keys(c!.context).length > 0 && (
              <Section title="Handoff context">
                <Facts rows={Object.entries(c!.context).map(([k, v]) => [cap(plain(k)), typeof v === "object" ? JSON.stringify(v) : String(v)])} />
              </Section>
            )}
          </aside>
        </>
      )}
    </Listed>
  );
}

function Audio({ id, url }: { id: string; url: string }) {
  const [why, setWhy] = useState("");
  const retried = useRef(false);
  return (
    <>
      <audio
        controls
        preload="none"
        src={url}
        className="w-full"
        onPlaying={() => ((retried.current = false), setWhy(""))}
        onError={(e) => {
          const el = e.currentTarget;
          if (retried.current) return setWhy("The recording did not load. Reload the page and try again.");
          retried.current = true;
          api<Recording>("GET", `/api/v1/recordings/${id}`).then(
            (r) => ((el.src = r.audio_url), el.play().catch(() => setWhy("Press play again."))),
            (err) => setWhy(reason(err)),
          );
        }}
      />
      {why && <p className="text-s-400 text-destructive">{why}</p>}
    </>
  );
}
