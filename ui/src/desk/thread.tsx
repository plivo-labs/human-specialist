import { Phone } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { type Page, asAgent, every } from "../api";
import { CHANNEL } from "../conversations";
import { cap, phone, when } from "../format";
import type { Message, Task } from "../types";
import { Button, Facts, Textarea, Timeline, act, failed, useLoad } from "../ui";
import { describe } from "../words";
import { type Ctx, callOut, cantCall } from "./desk";
import { type Line, replySubject, thread, windowShut } from "./state";

type Name = (id?: string | null, fallback?: string) => string;

export function Thread({ ctx, line, name }: { ctx: Ctx; line: Line; name: Name }) {
  const { s, now, loadLine } = ctx;
  const cid = line.cid;
  const feed = s.feed.filter((e) => e.conversation_id === cid).reverse();
  const heard = feed.findLast((e) => e.type.startsWith("message."))?.id;
  const load = useLoad(() => every<Message>((c) => asAgent<Page<Message>>("GET", `/api/v1/conversations/${cid}/messages?limit=200${c}`)), [cid, heard]);
  const [text, setText] = useState("");
  const end = useRef<HTMLDivElement>(null);
  const msgs = load.data ?? [];
  const rows = thread(msgs, feed, (e) => describe(e, name, s.me.id));
  useEffect(() => void end.current?.scrollIntoView({ block: "end" }), [rows.length]);
  const shut = !!load.data && windowShut(line.conv.channel, msgs, now);
  const subject = line.conv.channel === "email" ? replySubject(msgs) : null;
  const send = async () => {
    const said = text.trim();
    if (!said) return;
    setText("");
    await asAgent("POST", `/api/v1/conversations/${cid}/messages`, { text: said, ...(subject && { email: { subject } }) }).catch((e) => (setText(said), failed("Sending", e)));
  };
  return (
    <>
      <Timeline start rows={rows} />
      {load.error && <p className="text-s-400 text-destructive">{load.error}</p>}
      <div className="sticky bottom-0 mt-auto grid gap-2 border-t bg-background pt-3">
        {shut ? (
          <p className="text-s-400 text-secondary">WhatsApp takes a free reply only within 24 hours of the customer's last message. After that it needs an approved template, which this desk cannot send.</p>
        ) : (
          <Textarea
            aria-label="Reply"
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && (e.preventDefault(), send())}
            placeholder={subject ? `Reply by email (${subject})` : `Reply by ${CHANNEL[line.conv.channel]}. Shift+Enter adds a line.`}
          />
        )}
        <div className="flex justify-between gap-2">
          <Button variant="danger" onClick={async () => (await act("Ending the conversation", () => asAgent("POST", `/api/v1/conversations/${cid}/close`), "Conversation ended. Wrap-up is open.")) && loadLine(cid)}>
            End conversation
          </Button>
          <Button variant="primary" disabled={shut || !text.trim()} onClick={send}>
            Send
          </Button>
        </div>
      </div>
      <div ref={end} />
    </>
  );
}

export function Work({ ctx, line, name }: { ctx: Ctx; line: Line; name: Name }) {
  const { s, loadLine } = ctx;
  const load = useLoad(() => asAgent<Page<Task>>("GET", `/api/v1/tasks?conversation_id=${line.cid}&limit=1`), [line.cid]);
  const t = load.data?.data[0];
  const why = t?.customer ? cantCall(s) : "";
  const rows = thread([], s.feed.filter((e) => e.conversation_id === line.cid).reverse(), (e) => describe(e, name, s.me.id));
  const resolve = () => act("Resolving the task", () => asAgent("PATCH", `/api/v1/tasks/${t!.id}`, { status: "resolved" }), "Task resolved. Wrap-up is open.");
  return (
    <>
      {load.error && <p className="text-s-400 text-destructive">{load.error}</p>}
      {t?.description && <p className="whitespace-pre-wrap text-p-400">{t.description}</p>}
      {t && <Facts rows={[["Priority", cap(t.priority)], ["Due", t.due_at ? when(t.due_at) : "None"]]} />}
      <Timeline start rows={rows} />
      <div className="sticky bottom-0 mt-auto grid gap-2 border-t bg-background pt-3">
        {why && <p className="text-s-400 text-secondary">{why}</p>}
        <div className="flex justify-between gap-2">
          {t?.customer ? (
            <Button disabled={!!why} onClick={() => callOut(ctx, t.customer!, line.cid)}>
              <Phone aria-hidden /> Call {phone(t.customer)}
            </Button>
          ) : (
            <span />
          )}
          <Button variant="primary" disabled={!t} onClick={async () => (await resolve()) && loadLine(line.cid)}>
            Resolve
          </Button>
        </div>
      </div>
    </>
  );
}
