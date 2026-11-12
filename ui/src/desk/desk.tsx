import * as Menu from "@radix-ui/react-dropdown-menu";
import { ChevronDown, Phone as Dial } from "lucide-react";
import { type ReactNode, useEffect, useReducer, useRef, useState } from "react";
import { ApiError, BASE, type Page, asAgent, every, hold, reloadToSignIn, renew, renewals } from "../api";
import { CHANNEL } from "../conversations";
import { phone as number, plain, time } from "../format";
import { NO_PASSWORD, renewDue } from "../held";
import { Dropdown, go, menuItem } from "../shell";
import type { Conversation, ConversationSummary, Disposition, Envelope, Me, PresenceStatus, Session, Status, TransferTargets } from "../types";
import { Button, Card, Field, Form, Input, Lamp, Listed, Modal, Notice, Submit, act, cls, failed, useLoad } from "../ui";
import { type Phone, startPhone } from "./phone";
import { CallWidget, Offers } from "./stage";
import { type Action, type State, TASK, autoAnswer, frames, init, namesMe, onDial, onLine, reduce, sdkFree, stalled } from "./state";

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
    if (conv && TASK.has(conv.channel))
      asAgent<Page<{ title: string }>>("GET", `/api/v1/tasks?conversation_id=${cid}&limit=1`).then((t) => t.data[0] && dispatch({ type: "names", names: { [cid]: t.data[0].title } }), () => {});
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

export const Desked = ({ ctx, children }: { ctx: Ctx; children: ReactNode }) => (
  <div className="flex h-full flex-col">
    <Warnings s={ctx.s} dispatch={ctx.dispatch} ends={ctx.ends} now={ctx.now} />
    <Offers ctx={ctx} />
    <CallWidget ctx={ctx} />
    <div className="min-h-0 flex-1 overflow-auto">{children}</div>
  </div>
);

export const tint = (st?: PresenceStatus) => (st?.availability ? "var(--available)" : (st?.color ?? "var(--tertiary)"));

export function StatusMenu({ ctx: { s, dispatch } }: { ctx: Ctx }) {
  const current = s.statuses.find((x) => x.id === s.me.status?.id);
  return (
    <Dropdown
      label="Your status"
      onOpen={() => statusList().then((statuses) => dispatch({ type: "statuses", statuses }), () => {})}
      className="max-h-[170px] w-[222px]"
      trigger={
        <Button size="sm" className="gap-2">
          <Lamp color={tint(current)} />
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
          <Lamp color={tint(st)} />
          {st.name}
        </Menu.Item>
      ))}
    </Dropdown>
  );
}

export function cantCall(s: State) {
  const off = s.me.role === "admin" ? "Turn it on under Settings, Outbound Configurations." : "An admin turns it on under Settings, Outbound Configurations.";
  if (!s.me.settings.agent_direct_dial) return `Calling out is off. ${off}`;
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
      <Button className={cls(why && "opacity-60")} title={why || undefined} onClick={() => setOpen(true)}>
        <Dial aria-hidden /> Call
      </Button>
      <Modal open={open} onClose={() => setOpen(false)} title="Outgoing call">
        {why ? (
          <p className="pb-2 text-p-400 text-secondary">{why}</p>
        ) : (
          <Form onSubmit={(f) => dial(String(f.get("to")).trim())}>
            <Field label="Number" hint="This browser connects first, then the customer's phone rings.">
              <Input name="to" required autoFocus inputMode="tel" placeholder="+14155550100" pattern="\+\d{6,15}" title="A plus sign, then the country code and number" />
            </Field>
            <Submit className="justify-self-end">Call</Submit>
          </Form>
        )}
      </Modal>
    </>
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
