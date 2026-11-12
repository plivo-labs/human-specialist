import { Headphones, Mic, MicOff, Pause, Phone, PhoneForwarded, PhoneIncoming, PhoneOutgoing, UserRoundPlus, Users } from "lucide-react";
import { type ReactNode, useState } from "react";
import { asAgent } from "../api";
import { kindIcon, kindWord } from "../conversations";
import { clock, phone } from "../format";
import type { Conversation } from "../types";
import { Badge, Button, act, cls, useLoad } from "../ui";
import type { Ctx } from "./desk";
import { left, titled } from "./side";
import { type Line, type Pending, pending, stage } from "./state";
import { Transfer, TransferBar } from "./transfer";

export const hangUp = <Phone className="rotate-[135deg] fill-destructive stroke-none" aria-hidden />;

function Card({ ring, children }: { ring?: { left: number | null; of: number | null }; children: ReactNode }) {
  return (
    <section className="w-full overflow-hidden rounded-sm border bg-background px-4 pb-3 pt-3 shadow-lg sm:w-80">
      {ring && (
        <div aria-hidden className="-mx-4 -mt-3 mb-3 h-[3px] bg-muted">
          {ring.left != null && <div className="h-full bg-available transition-[width] duration-1000 ease-linear" style={{ width: `${(ring.left / ring.of!) * 100}%` }} />}
        </div>
      )}
      <div className="flex flex-col gap-2">{children}</div>
    </section>
  );
}

const Title = ({ icon, team, children }: { icon: ReactNode; team?: string | null; children: ReactNode }) => (
  <div className="flex items-center justify-between gap-2 text-xs-400 text-secondary">
    <span className="flex items-center gap-1 [&_svg]:size-4">
      {icon}
      <span>{children}</span>
    </span>
    {team && (
      <span className="flex items-center gap-1">
        <Users className="size-4 text-tertiary" aria-hidden />
        {team}
      </span>
    )}
  </div>
);

const Who = ({ number, title, held, sub }: { number: string | null; title?: string; held?: boolean; sub: ReactNode }) => (
  <div className="ml-5 flex flex-col">
    <span className="flex items-start justify-between gap-1 text-p-600">
      {title ?? phone(number)}
      <span className={cls(!held && "invisible")}>
        <Badge warn>On Hold</Badge>
      </span>
    </span>
    <span className="text-p-400 text-tertiary" role="status">
      {sub}
    </span>
  </div>
);

const To = ({ out, text, address }: { out?: boolean; text?: boolean; address?: string | null }) =>
  address ? (
    <span className="truncate">
      {out ? "Calling from" : text ? "To" : "Call to"} <span className="text-xs-600">{address}</span>
    </span>
  ) : (
    <span />
  );

export function Offers({ ctx }: { ctx: Ctx }) {
  const offers = pending(ctx.s);
  const ringing = offers.at(-1);
  return (
    <>
      <p className="sr-only" aria-live="assertive">
        {ringing ? `${ringing.via === "transfer" ? "Incoming transfer" : kindWord({ ...ringing, direction: "inbound" })} from ${phone(ringing.from)}` : ""}
      </p>
      {offers.length > 0 && (
        <div className="flex max-h-[45%] shrink-0 flex-wrap gap-2 overflow-y-auto border-b bg-muted/40 p-2">
          {offers.map((o) => (
            <Offer key={o.id} ctx={ctx} offer={o} />
          ))}
        </div>
      )}
    </>
  );
}

export function CallWidget({ ctx }: { ctx: Ctx }) {
  const st = stage(ctx.s);
  if (st.kind === "idle") return null;
  return (
    <div className="flex shrink-0 flex-wrap gap-2 border-b bg-muted/40 p-2">
      {st.kind === "dialing" && (
        <Card>
          <Title icon={<PhoneOutgoing aria-hidden />}>Outgoing call</Title>
          <Who number={st.dial.to} sub="Connecting..." />
        </Card>
      )}
      {st.kind === "live" && <LiveCall ctx={ctx} line={st.line} />}
      {st.kind === "monitor" && <Monitor ctx={ctx} line={st.line} />}
    </div>
  );
}

function Offer({ ctx, offer }: { ctx: Ctx; offer: Pending }) {
  const { s, now, phone: sdk, dispatch, loadLine } = ctx;
  const t = left(offer, now);
  const ring = offer.ring;
  const text = offer.channel !== "call";
  const transfer = offer.via === "transfer";
  const conv = useLoad(() => asAgent<Conversation>("GET", `/api/v1/conversations/${offer.cid}`), [offer.cid]);
  const target = offer.to ?? conv.data?.our_address ?? null;
  const address = target ? phone(target) : s.names[offer.queueId ?? ""];
  const waited = !transfer && conv.data ? clock((now - Date.parse(conv.data.created_at)) / 1000) : null;
  const why = text ? (t == null ? "Waiting for you" : `${clock(t)} left to accept`) : ring ? (t == null ? "Ringing" : `Ringing, ${Math.ceil(t)} s left to answer`) : s.phone.state === "ready" ? "Connecting the ring to this browser…" : "This browser's phone is not ready, so you cannot answer here.";
  const accept = text
    ? async () => (await act("Accepting", () => asAgent("POST", `/api/v1/offers/${offer.id}/accept`), "Accepted")) && loadLine(offer.cid)
    : () => act("Answering", () => sdk.current!.answer(ring!.uuid), "Call connected");
  return (
    <Card ring={{ left: t, of: offer.ringFor }}>
      <div className="flex justify-between gap-2 text-xs-400 text-tertiary">
        <To text={text} address={address} />
        {waited && <span>Waiting {waited}</span>}
      </div>
      <Title icon={transfer ? <PhoneForwarded aria-hidden /> : text ? kindIcon(offer) : <PhoneIncoming aria-hidden />} team={ring?.team}>
        {transfer ? (
          <>
            {text ? "Transfer" : "Call transfer"} from <span className="text-xs-600">{ring?.fromagent ?? "a colleague"}</span>
          </>
        ) : (
          kindWord({ ...offer, direction: "inbound" })
        )}
      </Title>
      <Who number={offer.from} title={titled(s.names, { id: offer.cid, channel: offer.channel, number: offer.from })} sub={why} />
      {text && conv.data?.last_message?.text && <p className="ml-5 line-clamp-3 whitespace-pre-wrap text-p-400">{conv.data.last_message.text}</p>}
      <div className="flex items-center justify-around">
        <Button
          variant="danger"
          className="w-32 rounded-full"
          disabled={!text && (!ring || !sdk.current)}
          onClick={() =>
            act(text ? "Declining" : "Rejecting", async () => {
              if (!text) sdk.current!.reject(ring!.uuid);
              dispatch({ type: "decline", offer: offer.id, uuid: ring?.uuid ?? "" });
              if (text) await asAgent("POST", `/api/v1/offers/${offer.id}/decline`);
            }, `${text ? "Declined" : "Call rejected"}. It goes to someone else.`)
          }
        >
          {hangUp} {text ? "Decline" : "Reject"}
        </Button>
        <Button variant="success" className="w-32 rounded-full" disabled={!text && (!ring || !sdk.current)} onClick={accept}>
          {text ? kindIcon(offer) : <PhoneIncoming aria-hidden />} Accept
        </Button>
      </div>
    </Card>
  );
}

const tool = (label: string, pressed?: boolean) => ({ variant: "tertiary" as const, size: "icon" as const, "aria-label": label, title: label, "aria-pressed": pressed });

function LiveCall({ ctx, line }: { ctx: Ctx; line: Line }) {
  const { s, dispatch, now, phone: sdk } = ctx;
  const [transferring, setTransferring] = useState(false);
  const c = line.conv;
  const cid = line.cid;
  const out = c.direction === "outbound";
  const ringing = s.dialing?.cid === cid;
  const joined = c.channel !== "call";
  const rec = s.me.settings.recording;
  const from = s.names[c.assignments.find((a) => a.role === "primary" && a.agent_id !== s.me.id)?.agent_id ?? ""] ?? "a colleague";
  const patch = (p: Partial<Line>) => dispatch({ type: "patch", cid, patch: p });
  return (
    <Card>
      <div className="flex items-center justify-between gap-2 text-xs-400 text-tertiary">
        <To out={out} address={c.our_address && phone(c.our_address)} />
        {!ringing && !joined && rec?.allow_pause && rec[c.direction] && (
          <button
            type="button"
            className={cls("shrink-0 text-xxs-600 uppercase underline", line.paused ? "text-tertiary" : "text-destructive")}
            onClick={async () => {
              const action = line.paused ? "resume" : "pause";
              if (await act(action === "pause" ? "Pausing the recording" : "Resuming the recording", () => asAgent("POST", `/api/v1/calls/${cid}/recording`, { action }), action === "pause" ? "Recording paused" : "Recording resumed"))
                patch({ paused: action === "pause" });
            }}
          >
            {line.paused ? "Resume rec" : "Pause rec"}
          </button>
        )}
      </div>
      <Title icon={out ? <PhoneOutgoing aria-hidden /> : line.via === "transfer" ? <PhoneForwarded aria-hidden /> : <PhoneIncoming aria-hidden />} team={c.queue_name}>
        {out ? "Outgoing call" : line.consult ? `Consulting ${from}, caller on hold` : line.via === "transfer" ? `Call transferred from ${from}` : "Incoming call"}
      </Title>
      <Who number={c.number} held={line.held} sub={ringing ? "Ringing…" : clock((now - line.started) / 1000)} />
      <div className="ml-5 flex items-center gap-2">
        {s.phone.state === "ready" && sdk.current && (
          <Button
            {...tool(line.muted ? "Unmute" : "Mute", line.muted)}
            onClick={() =>
              act(line.muted ? "Unmuting" : "Muting", async () => {
                sdk.current!.mute(!line.muted);
                patch({ muted: !line.muted });
              }, line.muted ? "Unmuted" : "Muted. The caller cannot hear you.")
            }
          >
            {line.muted ? <MicOff aria-hidden /> : <Mic aria-hidden />}
          </Button>
        )}
        {!ringing && !joined && (
          <>
            <Button
              {...tool(line.held ? "Unhold" : "Hold", line.held)}
              onClick={async () => {
                const on = !line.held;
                if (await act(on ? "Putting the caller on hold" : "Taking the caller off hold", () => asAgent("POST", `/api/v1/calls/${cid}/hold`, { on }), on ? "Caller on hold" : "Caller off hold")) patch({ held: on });
              }}
            >
              <Pause aria-hidden />
            </Button>
            <Button {...tool("Transfer", transferring)} disabled={!!line.transfer} onClick={() => setTransferring(!transferring)}>
              <UserRoundPlus aria-hidden />
            </Button>
          </>
        )}
        <Button
          variant="danger"
          size="icon"
          aria-label={ringing ? "Cancel the call" : "End call"}
          title="End call"
          onClick={() => act("Hanging up", async () => void (joined ? sdk.current?.hangup() : await asAgent("POST", `/api/v1/calls/${cid}/hangup`)), ringing ? "Call cancelled" : joined ? "Call ended" : "Call ended. Wrap-up is open.")}
        >
          {hangUp}
        </Button>
      </div>
      {line.transfer && <TransferBar ctx={ctx} line={line} />}
      {transferring && !line.transfer && <Transfer ctx={ctx} line={line} onDone={() => setTransferring(false)} />}
    </Card>
  );
}

const MODES = [
  { mode: "listen", label: "Listen", icon: null, hint: "You are listening. Nobody on the call hears you." },
  { mode: "whisper", label: "Coach", icon: <Headphones aria-hidden />, hint: "You are coaching. Only the agent hears you." },
  { mode: "barge", label: "Barge-in", icon: <UserRoundPlus aria-hidden />, hint: "You are on the call. The agent and the caller hear you." },
];

function Monitor({ ctx, line }: { ctx: Ctx; line: Line }) {
  const { s, dispatch, now } = ctx;
  const cid = line.cid;
  const modes = MODES.filter((m) => m.mode !== line.monitor && (m.mode !== "barge" || s.me.grants.barge));
  return (
    <Card>
      <Title icon={<Headphones aria-hidden />} team={line.conv.queue_name}>
        Monitoring
      </Title>
      <Who number={line.conv.number} sub={clock((now - line.started) / 1000)} />
      <div className="ml-5 flex items-center gap-2">
        {modes.map(({ mode, label, icon, hint }) => (
          <Button
            key={mode}
            className="min-w-0 flex-1 rounded-full px-2"
            title={hint}
            onClick={() =>
              act(`Switching to ${label.toLowerCase()}`, () => asAgent("PATCH", `/api/v1/calls/${cid}/monitor`, { mode }), () => {
                dispatch({ type: "patch", cid, patch: { monitor: mode } });
                return hint;
              })
            }
          >
            {icon}
            {label}
          </Button>
        ))}
        <Button variant="danger" size="icon" aria-label="End monitoring session" title="End monitoring session" onClick={() => act("Leaving the call", () => asAgent("DELETE", `/api/v1/calls/${cid}/monitor`), "You left the call")}>
          {hangUp}
        </Button>
      </div>
      <p className="-mx-4 -mb-3 mt-2 bg-muted px-4 py-2 text-xs-400 text-secondary">{MODES.find((m) => m.mode === line.monitor)?.hint}</p>
    </Card>
  );
}
