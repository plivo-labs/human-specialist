import * as Menu from "@radix-ui/react-dropdown-menu";
import { ChevronDown, MessageSquare, Phone as Dial } from "lucide-react";
import { type ReactNode, useEffect, useReducer, useRef, useState } from "react";
import { ApiError, BASE, type Page, asAgent, every, hold, reloadToSignIn, renew, renewals } from "../api";
import { CHANNEL } from "../conversations";
import { ago, hue, phone as number, plain, time } from "../format";
import { NO_PASSWORD, renewDue } from "../held";
import { Dropdown, go, menuItem } from "../shell";
import type { Conversation, ConversationSummary, Destination, Disposition, Envelope, Me, PresenceStatus, Session, Status, Task, Template, TransferTargets } from "../types";
import { Button, Card, Field, Form, Input, Lamp, Listed, Modal, Notice, PhoneInput, Select, Submit, Textarea, act, cls, failed, useLoad } from "../ui";
import { type Phone, startPhone } from "./phone";
import { CallWidget, Offers } from "./stage";
import { fill, holes } from "./template";
import { type Action, type State, TASK, autoAnswer, frames, history, init, namesMe, onDial, onLine, own, recent, recentCalls, reduce, sdkFree, stalled } from "./state";

export type Ctx = { s: State; dispatch: (a: Action) => void; phone: { current: Phone | null }; loadLine: (cid: string) => Promise<void>; loadAll: () => void; now: number; ends: number };
type Render = { session: Session; children: (ctx: Ctx) => ReactNode };

const FOLLOW = new Set(["offer.created", "offer.accepted", "call.answered", "supervisor.joined", "conversation.wrap_up_started"]);
const ANSWER: Record<string, [string, string]> = {
  outbound: ["Connecting your phone", "Your phone is connected. The customer's phone rings next."],
  monitor: ["Joining the call", "You are on the call"],
};

export function useNow(ms = 1000) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), ms);
    return () => clearInterval(t);
  }, [ms]);
  return now;
}

const statusList = () => every<PresenceStatus>((c) => asAgent<Page<PresenceStatus>>("GET", `/api/v1/agent-statuses?limit=200${c}`));

export function Desk({ session, children }: Render) {
  const boot = useLoad(() =>
    Promise.all([
      asAgent<Me>("GET", `/api/v1/me`),
      statusList(),
      every<Disposition>((c) => asAgent<Page<Disposition>>("GET", `/api/v1/dispositions?limit=200${c}`)),
    ]),
  );
  if (boot.data) return <Live session={session} boot={boot.data}>{children}</Live>;
  return (
    <Card>
      <Listed load={{ ...boot, data: undefined, error: boot.error && `The desk could not load. ${boot.error}` }} empty={null}>
        {() => null}
      </Listed>
    </Card>
  );
}

function Live({ session: first, children, boot: [me, statuses, dispositions] }: Render & { boot: [Me, PresenceStatus[], Disposition[]] }) {
  const [session, setSession] = useState(first);
  const [s, dispatch] = useReducer(reduce, undefined, () => init(me, statuses, dispositions));
  const ref = useRef(s);
  ref.current = s;
  const phone = useRef<Phone | null>(null);
  const asked = useRef<Record<string, number>>({});
  const timed = useRef(new Set<string>());
  const tried = useRef(0);
  const lastEvent = useRef("");
  const sdkToken = useRef(first.plivo.access_token);
  const now = useNow();
  const ends = Date.parse(session.expires_at);

  const loadLine = async (cid: string) => {
    const n = (asked.current[cid] = (asked.current[cid] ?? 0) + 1);
    const conv = await asAgent<Conversation>("GET", `/api/v1/conversations/${cid}`).catch((e) => {
      if (e instanceof ApiError && e.status === 404) return null;
      if (n === asked.current[cid]) {
        failed("Loading the conversation", e);
        dispatch({ type: "patch", cid, patch: { stale: true } });
      }
    });
    if (n === asked.current[cid] && conv !== undefined) dispatch({ type: "line", cid, conv });
    const mine = !!conv?.assignments.some((a) => a.agent_id === me.id);
    if (n === asked.current[cid] && conv !== undefined && !mine) timed.current.delete(cid);
    if (mine && !timed.current.has(cid)) {
      timed.current.add(cid);
      history((after) => asAgent<Page<Envelope>>("GET", `/api/v1/conversations/${cid}/timeline?after=${after}&limit=200`), () => timed.current.has(cid)).then(
        (events) => dispatch({ type: "history", events }),
        (e) => {
          if (e instanceof ApiError && e.status < 500) return;
          timed.current.delete(cid);
          failed("Loading the activity", e);
        },
      );
    }
    if (conv && TASK.has(conv.channel))
      asAgent<Page<Task>>("GET", `/api/v1/tasks?conversation_id=${cid}&limit=50`).then((t) => own(t.data, cid)).then((mine) => mine && dispatch({ type: "names", names: { [cid]: mine.title } }), () => {});
  };

  const loadAll = () =>
    asAgent<Page<ConversationSummary>>("GET", `/api/v1/conversations?participant_agent_id=${me.id}&limit=50`).then(
      (held) => new Set([...held.data.map((c) => c.id), ...ref.current.lines.map((l) => l.cid)]).forEach((cid) => loadLine(cid)),
      (e) => failed("Finding your conversations", e),
    );

  const renewNow = () => {
    tried.current = Date.now();
    renew()
      .then(async (fresh) => {
        hold({ session: fresh });
        setSession(fresh);
        dispatch({ type: "renewed", me: await asAgent<Me>("GET", `/api/v1/me`) });
      })
      .catch((e) => {
        const ended = e instanceof ApiError && e.code === "sign_in_required";
        if (ended) tried.current = Infinity;
        failed("Renewing your session", ended ? new Error(`This session cannot renew, so sign in again. ${NO_PASSWORD}`) : e);
      });
  };
  useEffect(() => void (renewDue(ends, now, tried.current) && renewNow()), [now]);
  useEffect(() => (renewals.addEventListener("now", renewNow), () => renewals.removeEventListener("now", renewNow)), []);

  useEffect(() => {
    if (!phone.current || sdkToken.current === session.plivo.access_token || !sdkFree(s)) return;
    sdkToken.current = session.plivo.access_token;
    phone.current.login(sdkToken.current);
  });

  useEffect(() => void (s.focus && go(`/desk/${s.focus.split("/")[0]}`)), [s.focus]);

  useEffect(() => {
    let live = true;
    startPhone(session.plivo.access_token, {
      state: (state, why) => dispatch({ type: "phone", state, why }),
      ring: (r) => {
        dispatch({ type: "ring", ring: r });
        if (!autoAnswer(r)) return;
        const [label, done] = ANSWER[r.kind ?? ""] ?? ["Answering", "Call connected"];
        setTimeout(() => phone.current && act(label, () => phone.current!.answer(r.uuid), done), 1000 * (Number(r.autoanswer) || 0));
      },
      gone: (uuid) => dispatch({ type: "ring-gone", uuid }),
      answered: (uuid) => {
        const r = ref.current.rings.find((x) => x.uuid === uuid);
        dispatch({ type: "ring-gone", uuid });
        if (r?.conversation) loadLine(r.conversation);
      },
    }).then((p) => live && (phone.current = p));
    loadAll();
    me.pending_offers.forEach((o) => loadLine(o.conversation_id));
    asAgent<TransferTargets>("GET", `/api/v1/transfer-targets?channel=call`).then(
      (t) => dispatch({ type: "names", names: Object.fromEntries([...t.agents, ...t.teams].map((x) => [x.id, x.name ?? "Unnamed"])) }),
      (e) => failed("Loading your colleagues", e),
    );
    return () => void (live = false);
  }, []);

  useEffect(() => {
    let open = new AbortController();
    let last = Date.now();
    let done = false;
    const watch = setInterval(() => {
      if (!stalled(last, Date.now())) return;
      last = Date.now();
      dispatch({ type: "stream", state: "stopped" });
      open.abort();
    }, 5000);
    const on = (f: { id: string; data: string }) => {
      lastEvent.current = f.id || lastEvent.current;
      const e: Envelope = JSON.parse(f.data);
      if (e.type === "auth.expired") return reloadToSignIn();
      const was = ref.current;
      dispatch({ type: "event", e });
      if (e.conversation_id && (onLine(was, e) || onDial(was, e) || (namesMe(was, e) && FOLLOW.has(e.type)))) loadLine(e.conversation_id);
      if (onDial(was, e) && e.type === "call.failed" && e.data.result !== "cancelled")
        failed(`Calling ${number(was.dialing!.to)}`, new Error(`The call did not connect: ${plain(e.data.result)}.`));
    };
    (async () => {
      while (!done) {
        open = new AbortController();
        try {
          const after = lastEvent.current && `?after=${lastEvent.current}`;
          const res = await fetch(`${BASE}/api/v1/events/stream${after}`, { headers: { Authorization: `Bearer ${session.stream_token}` }, signal: open.signal });
          if (res.status === 401) return reloadToSignIn();
          if (!res.ok) throw new Error(`The server answered ${res.status}`);
          const reader = res.body!.pipeThrough(new TextDecoderStream()).getReader();
          for (let buf = "", r = await reader.read(); !r.done; r = await reader.read()) {
            last = Date.now();
            const was = ref.current;
            if (was.stream !== "live") dispatch({ type: "stream", state: "live" });
            if (was.stream === "stopped") (loadAll(), was.dialing && loadLine(was.dialing.cid));
            const read = frames(buf + r.value);
            buf = read.rest;
            read.events.forEach(on);
          }
        } catch (e) {
          if (!(e instanceof DOMException && e.name === "AbortError")) console.warn("The event stream failed", e);
        }
        await new Promise((r) => setTimeout(r, 3000));
      }
    })();
    return () => {
      done = true;
      clearInterval(watch);
      open.abort();
    };
  }, [session.stream_token]);

  useEffect(() => {
    const w = new Worker(new URL("./heartbeat.ts", import.meta.url), { type: "module" });
    let beats = 0;
    w.onmessage = ({ data }) => {
      if (data === 401) return reloadToSignIn();
      dispatch({ type: "reachable", ok: data !== 0 });
      if (++beats === 2) asAgent<Me>("GET", `/api/v1/me`).then((m) => m.status && dispatch({ type: "status", status: m.status }), () => {});
    };
    w.postMessage(["POST", `/api/v1/me/heartbeat`, BASE, session.agent_token]);
    return () => w.terminate();
  }, [session.agent_token]);

  return children({ s, dispatch, phone, loadLine, loadAll, now, ends });
}

// On the inbox at md and wider the cards top the My Conversations column, scrolling so the list keeps 14rem: the
// page's columns join this grid. Elsewhere both wrappers are `contents`, so the cards size against the desk as before.
export const Desked = ({ ctx, inbox, children }: { ctx: Ctx; inbox: boolean; children: ReactNode }) => (
  <div className="flex h-full flex-col">
    <Warnings s={ctx.s} dispatch={ctx.dispatch} ends={ctx.ends} now={ctx.now} />
    <div className={cls("contents", inbox && "md:grid md:min-h-0 md:flex-1 md:grid-cols-[315px_minmax(0,1fr)_auto] md:grid-rows-[minmax(0,auto)_minmax(14rem,1fr)]")}>
      <div className={cls("contents", inbox && "md:col-start-1 md:row-start-1 md:block md:overflow-y-auto md:border-r md:border-st2 md:*:max-h-none")}>
        <Offers ctx={ctx} />
        <CallWidget ctx={ctx} />
      </div>
      <div className={cls("min-h-0 flex-1 overflow-auto", inbox && "md:contents")}>{children}</div>
    </div>
  </div>
);

export function StatusMenu({ ctx: { s, dispatch } }: { ctx: Ctx }) {
  const current = s.statuses.find((x) => x.id === s.me.status?.id);
  return (
    <Dropdown
      label="Your status"
      onOpen={() => statusList().then((statuses) => dispatch({ type: "statuses", statuses }), () => {})}
      className="max-h-[170px] w-[222px]"
      trigger={
        <Button size="sm" className="gap-2">
          <Lamp color={hue(current)} />
          {current?.name ?? s.me.status?.name ?? "Offline"}
          <ChevronDown className="opacity-60" aria-hidden />
        </Button>
      }
    >
      {[...s.statuses].sort((a, b) => a.sequence - b.sequence).map((st) => (
        <Menu.Item
          key={st.id}
          className={cls(menuItem, st.id === s.me.status?.id && "bg-muted")}
          onSelect={() =>
            act("Changing your status", () => asAgent<{ status: Status }>("PUT", `/api/v1/me/status`, { status_id: st.id }), (r) => {
              dispatch({ type: "status", status: r.status });
              return `Your status is now ${r.status.name}`;
            })
          }
        >
          <Lamp color={hue(st)} />
          {st.name}
        </Menu.Item>
      ))}
    </Dropdown>
  );
}

const off = (s: State) => (s.me.role === "admin" ? "Turn it on under Settings, Outbound Configurations." : "An admin turns it on under Settings, Outbound Configurations.");

export function cantCall(s: State) {
  if (!s.me.settings.agent_direct_dial) return `Calling out is off. ${off(s)}`;
  if (s.lines.some((l) => l.conv.channel === "call" || l.via === "outbound")) return "Finish the current call first.";
  return s.phone.state === "ready" ? "" : "Calling needs this browser's phone, which is not ready. The notice above says why.";
}

export async function callOut({ s, dispatch, loadLine }: Ctx, to: string, conversation_id?: string) {
  const made = await act(`Calling ${number(to)}`, () => asAgent<{ conversation_id: string }>("POST", `/api/v1/calls`, { agent_id: s.me.id, to, conversation_id }), "Dialing");
  if (!made) return false;
  dispatch({ type: "dialing", dial: { cid: made.conversation_id, to } });
  loadLine(made.conversation_id);
  return true;
}

export function Dialer({ ctx }: { ctx: Ctx }) {
  const [open, setOpen] = useState(false);
  const why = cantCall(ctx.s);
  const dial = async (to: string) => (await callOut(ctx, to)) && setOpen(false);
  return (
    <>
      <Button className={cls("max-sm:px-2.5", why && "opacity-60")} title={why || undefined} onClick={() => setOpen(true)}>
        <Dial aria-hidden /> <span className="max-sm:sr-only">Call</span>
      </Button>
      <Modal open={open} onClose={() => setOpen(false)} title="Outgoing call">
        {why ? (
          <p className="pb-2 text-p-400 text-secondary">{why}</p>
        ) : (
          <>
            <Form onSubmit={(f) => dial(String(f.get("to")))}>
              <Field label="Number" hint="This browser connects first, then the customer's phone rings.">
                <PhoneInput name="to" required autoFocus />
              </Field>
              <Submit className="justify-self-end">Call</Submit>
            </Form>
            <Recent me={ctx.s.me.id} dial={dial} />
          </>
        )}
      </Modal>
    </>
  );
}

const cantMessage = (s: State) =>
  !s.me.settings.agent_direct_dial ? `Messaging out is off. ${off(s)}` : s.statuses.find((x) => x.id === s.me.status?.id)?.availability ? "" : "Set a status that takes conversations first.";

export function Messenger({ ctx }: { ctx: Ctx }) {
  const [open, setOpen] = useState(false);
  const [channel, setChannel] = useState("sms");
  const [tpl, setTpl] = useState<Filled>();
  const why = cantMessage(ctx.s);
  const ready = channel === "sms" || !!tpl?.values.every((v) => v.trim());
  const send = async (f: FormData) => {
    const to = String(f.get("to"));
    const content = channel === "sms" ? { text: f.get("text") } : { template: fill(tpl!.t, tpl!.values) };
    const made = await act(`Messaging ${number(to)}`, () => asAgent<{ conversation_id: string }>("POST", `/api/v1/conversations`, { channel, to, agent_id: ctx.s.me.id, ...content }), "Message sent");
    if (!made) return;
    setOpen(false);
    ctx.loadLine(made.conversation_id);
    go(`/desk/${made.conversation_id}`);
  };
  return (
    <>
      <Button className={cls("max-sm:px-2.5", why && "opacity-60")} title={why || undefined} onClick={() => setOpen(true)}>
        <MessageSquare aria-hidden /> <span className="max-sm:sr-only">Message</span>
      </Button>
      <Modal open={open} onClose={() => setOpen(false)} title="New message">
        {why ? (
          <p className="pb-2 text-p-400 text-secondary">{why}</p>
        ) : (
          <Form onSubmit={send}>
            <Field label="Channel">
              <Select value={channel} onChange={(e) => setChannel(e.target.value)}>
                <option value="sms">SMS</option>
                <option value="whatsapp">WhatsApp</option>
              </Select>
            </Field>
            <Field label="Number">
              <PhoneInput name="to" required autoFocus />
            </Field>
            {channel === "sms" ? (
              <Field label="Message">
                <Textarea name="text" required />
              </Field>
            ) : (
              <Templates tpl={tpl} set={setTpl} />
            )}
            <Submit className="justify-self-end" disabled={!ready}>
              Send
            </Submit>
          </Form>
        )}
      </Modal>
    </>
  );
}

type Filled = { t: Template; values: string[] };

function Templates({ tpl, set }: { tpl?: Filled; set: (f?: Filled) => void }) {
  const load = useLoad(() => every<Template>((c) => asAgent<Page<Template>>("GET", `/api/v1/whatsapp/templates?status=approved&limit=200${c}`)));
  // A pick lives only as long as the list it came from: closing the window or switching channel drops it.
  useEffect(() => () => set(undefined), []);
  return (
    <Listed load={{ ...load, error: load.error && `The WhatsApp templates did not load. ${load.error}` }} empty={<p className="text-s-400 text-secondary">No approved WhatsApp templates. They come from the WhatsApp accounts in the organization's settings.</p>}>
      {(all) => (
        <>
          <Field label="Template" hint={tpl?.t.components.find((c) => c.type.toLowerCase() === "body")?.text}>
            <Select
              required
              value={tpl?.t.id ?? ""}
              onChange={(e) => {
                const t = all.find((x) => x.id === e.target.value);
                set(t && { t, values: holes(t).map(() => "") });
              }}
            >
              <option value="">Pick an approved template</option>
              {all.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name} ({t.language})
                </option>
              ))}
            </Select>
          </Field>
          {tpl &&
            holes(tpl.t).map((h, i) => (
              <Field key={`${tpl.t.id}-${i}`} label={h.label}>
                <Input required type={h.media ? "url" : "text"} value={tpl.values[i]} onChange={(e) => set({ ...tpl, values: tpl.values.with(i, e.target.value) })} />
              </Field>
            ))}
        </>
      )}
    </Listed>
  );
}

function Recent({ me, dial }: { me: string; dial: (to: string) => Promise<unknown> }) {
  const load = useLoad(() =>
    Promise.all([
      recentCalls(asAgent, me),
      every<Destination>((c) => asAgent<Page<Destination>>("GET", `/api/v1/transfer-destinations?limit=200${c}`)).catch((e) => (console.warn("The contacts' names did not load", e), [])),
    ]),
  );
  return (
    <section aria-labelledby="recent-calls" className="mt-5 grid gap-1 border-t pt-4">
      <h3 id="recent-calls" className="text-s-500 text-secondary">Recent calls</h3>
      <Listed load={{ ...load, data: load.data && recent(load.data[0].data, load.data[1]) }} empty={<p className="text-s-400 text-secondary">No calls out yet.</p>}>
        {(calls) =>
          calls.map((c) => (
            <button key={c.id} type="button" className="flex min-h-9 items-center justify-between gap-2 rounded-sm px-2 py-1 text-left text-s-400 hover:bg-muted" aria-label={`Call ${c.name ?? number(c.number)} again`} onClick={() => dial(c.number!)}>
              <span className="grid">
                {c.name}
                <span className="font-mono">{number(c.number)}</span>
              </span>
              <span className="text-tertiary">{ago(c.created_at)}</span>
            </button>
          ))
        }
      </Listed>
    </section>
  );
}

function Warnings({ s, dispatch, ends, now }: { s: State; dispatch: Ctx["dispatch"]; ends: number; now: number }) {
  const at = time(new Date(ends).toISOString());
  const text = !isSecureContext
      ? "This page is not on HTTPS or localhost, so the browser blocks the microphone. Open the desk over HTTPS to take calls."
      : s.phone.state === "failed"
        ? `${s.phone.why} Calls cannot ring in this browser.`
        : s.unreachable
          ? "The desk cannot reach the server. Calls may not ring until it reconnects."
          : ends - now < 5 * 60e3
            ? `Your session ends at ${at}. Then sign in again to keep taking calls.`
            : "";
  return (
    <>
      {text && <Notice tone={s.phone.state === "failed" || !isSecureContext ? "danger" : "warning"}>{text}</Notice>}
      {s.notices.map((n) => (
        <Notice key={n.id}>
          {n.missed ? "Missed" : "Declined"} {CHANNEL[n.channel] ?? n.channel} from {number(n.from)}.{n.strike ? ` Missed rings in a row: ${n.strike}.` : ""}{" "}
          <button type="button" className="underline" onClick={() => dispatch({ type: "dismiss", id: n.id })}>
            Dismiss
          </button>
        </Notice>
      ))}
      {s.stream === "stopped" && <Notice>Live updates stopped, so this page may be out of date. The desk is reconnecting.</Notice>}
    </>
  );
}
