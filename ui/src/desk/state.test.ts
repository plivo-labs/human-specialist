import assert from "node:assert/strict";
import { test } from "node:test";
import { asks, autoWrap, createTask, endCall, frames, history, init, note, own, pending, recent, recentCalls, reduce, replySubject, sdkFree, stage, stalled, thread, voice, windowShut } from "./state.ts";

const me = { id: "me", name: "Tom", role: "agent", teams: [], status: null, caller_id: null, endpoint_username: null, grants: { monitor: false, barge: false }, settings: { agent_direct_dial: false }, pending_offers: [] } as const;
let seq = 0;
const ev = (type: string, data: object, extra: object = {}) => ({ id: String(++seq), type, created_at: "", conversation_id: "c1", agent_id: "me", data, ...extra });
const conv = (state: "active" | "wrap_up", role = "primary", channel = "call", extra: object = {}, id = "c1") => ({ id, channel, number: "+14155550142", assignments: [{ agent_id: "me", role, mode: null, state, started_at: "2026-11-09T10:00:00Z", ended_at: null, wrap_up_until: null, ...extra }] }) as any;

test("a call moves from ring to live to wrap-up to idle", () => {
  let s = init(me as any, [], []);
  assert.equal(stage(s).kind, "idle");

  s = reduce(s, { type: "event", e: ev("offer.created", { offer_id: "o1", channel: "call", from: "+14155550142", via: "queue", ring_timeout_sec: 30 }) });
  s = reduce(s, { type: "ring", ring: { uuid: "u1", from: "+14155550142", at: 0, offer: "o1", conversation: "c1" } });
  assert.equal(pending(s)[0].ring?.uuid, "u1", "the ring joins its offer");

  s = reduce(s, { type: "ring-gone", uuid: "u1" });
  s = reduce(s, { type: "line", cid: "c1", conv: conv("active") });
  assert.equal(stage(s).kind, "live");
  assert.equal(pending(s).length, 0, "the offer of the line on screen is not shown again");

  s = reduce(s, { type: "event", e: ev("call.held", { participant_id: "p9" }) });
  assert.equal(s.lines[0].held, false, "another participant's hold is not the caller's");
  s = reduce(s, { type: "event", e: ev("call.held", { participant_id: null }) });
  assert.equal(s.lines[0].held, true);

  s = reduce(s, { type: "patch", cid: "c1", patch: { transfer: { id: "t1", mode: "warm", label: "Leo", status: "initiated" } } });
  s = reduce(s, { type: "event", e: ev("call.transferred", { transfer_id: "t1", status: "accepted" }) });
  assert.equal(s.lines[0].transfer?.status, "accepted");
  s = reduce(s, { type: "event", e: ev("call.transferred", { transfer_id: "t1", status: "cancelled" }) });
  assert.equal(s.lines[0].transfer, null);

  s = reduce(s, { type: "patch", cid: "c1", patch: { stale: true } });
  s = reduce(s, { type: "line", cid: "c1", conv: conv("wrap_up") });
  assert.equal(s.lines[0].phase, "wrap");
  assert.equal(stage(s).kind, "idle", "a call in wrap-up has no call card");
  assert.equal(s.lines[0].stale, false, "a read that succeeds clears the out-of-date mark");
  assert.equal(s.lines[0].held, true, "flags survive a reread of the same conversation");

  s = reduce(s, { type: "line", cid: "c1", conv: null });
  assert.equal(stage(s).kind, "idle");
  assert.equal(s.feed.length, 5);
});

test("a line's timeline fills its activity once, in order, around the live events", () => {
  let s = reduce(init(me as any, [], []), { type: "line", cid: "c1", conv: conv("active") });
  s = reduce(s, { type: "event", e: ev("call.held", {}, { id: "70" }) });
  const before = [ev("conversation.started", {}, { id: "30" }), ev("message.received", {}, { id: "50" }), ev("call.held", {}, { id: "70" })];
  s = reduce(s, { type: "history", events: before as any });
  s = reduce(s, { type: "event", e: ev("call.held", {}, { id: "70" }) });
  s = reduce(s, { type: "event", e: ev("call.resumed", {}, { id: "80" }) });
  assert.deepEqual(s.feed.map((e) => e.id), ["80", "70", "30"], "newest first, each once, messages left to the thread");
});

test("a line's history follows the cursor to the end, at most ten reads", async () => {
  const asked: string[] = [];
  const pages: Record<string, { data: any[]; next_cursor: string | null }> = { "0": { data: [{ id: "1" }], next_cursor: "1" }, "1": { data: [], next_cursor: "9" }, "9": { data: [{ id: "12" }], next_cursor: null } };
  assert.deepEqual((await history(async (after) => (asked.push(after), pages[after]))).map((e) => e.id), ["1", "12"]);
  assert.deepEqual(asked, ["0", "1", "9"], "a page with no events still hands on its cursor");
  asked.length = 0;
  let held = true;
  await history(async (after) => (asked.push(after), (held = after === "0"), pages[after]), () => held);
  assert.deepEqual(asked, ["0", "1"], "it stops once the desk no longer holds the line");
  let reads = 0;
  await history(async (after) => (reads++, { data: [], next_cursor: String(Number(after) + 1) }));
  assert.equal(reads, 10, "a timeline that never ends stops after ten reads");
});

test("a supervisor's stint shows as monitoring", () => {
  let s = reduce(init(me as any, [], []), { type: "event", e: ev("supervisor.joined", { supervisor_id: "me", mode: "listen" }, { agent_id: "tom" }) });
  assert.equal(s.feed.length, 1, "the join names the agent monitored, and the supervisor in its data");
  s = reduce(s, { type: "line", cid: "c1", conv: conv("active", "monitor") });
  assert.equal(stage(s).kind, "monitor");
});

test("an outbound call shows as dialing until the customer answers", () => {
  let s = reduce(init(me as any, [], []), { type: "dialing", dial: { cid: "c1", to: "+14155550100" } });
  assert.equal(stage(s).kind, "dialing");
  s = reduce(s, { type: "line", cid: "c1", conv: null });
  assert.equal(stage(s).kind, "dialing", "the read 404s while the agent's own leg rings");
  s = reduce(s, { type: "line", cid: "c1", conv: conv("active") });
  assert.equal(s.dialing?.cid, "c1", "the agent is on, the customer still rings");
  s = reduce(s, { type: "event", e: ev("call.answered", { call_uuid: "u2" }, { direction: "outbound" }) });
  assert.equal(s.dialing, null);
  assert.equal(stage(s).kind, "live");

  s = reduce(init(me as any, [], []), { type: "dialing", dial: { cid: "c1", to: "+14155550100" } });
  s = reduce(s, { type: "event", e: ev("call.failed", { result: "no_answer" }) });
  assert.equal(stage(s).kind, "idle");
  assert.equal(s.feed.length, 1, "the failure shows in the activity");
});

test("the phone takes a new token only while no call rings, dials or is live", () => {
  const idle = init(me as any, [], []);
  assert.equal(sdkFree(idle), true);
  assert.equal(sdkFree(reduce(idle, { type: "event", e: ev("offer.created", { offer_id: "o1", channel: "call" }) })), false, "a ring is on its way");
  assert.equal(sdkFree(reduce(idle, { type: "ring", ring: { uuid: "u1", from: "+14155550142", at: 0 } })), false);
  assert.equal(sdkFree(reduce(idle, { type: "dialing", dial: { cid: "c1", to: "+14155550100" } })), false);
  const live = reduce(idle, { type: "line", cid: "c1", conv: conv("active") });
  assert.equal(sdkFree(live), false);
  assert.equal(sdkFree(reduce(live, { type: "line", cid: "c1", conv: conv("wrap_up") })), true, "wrap-up has no call on the phone");
});

test("the stream reader reads HS's frames across chunks, and 35 s without a byte is a dead stream", () => {
  const first = frames('retry: 3000\n: ping\n\nid: 7\ndata: {"type":"a"}\n\n: ping\n\nid: 8\ndata: {"ty');
  assert.deepEqual(first.events, [{ id: "7", data: '{"type":"a"}' }], "a ping or a retry hint is no event");
  assert.deepEqual(frames(`${first.rest}pe":"b"}\n\n`).events, [{ id: "8", data: '{"type":"b"}' }], "an event split over two chunks is read whole");
  assert.deepEqual(frames('data: {"type":"auth.expired"}\n\n').events, [{ id: "", data: '{"type":"auth.expired"}' }]);
  assert.equal(stalled(0, 34_999), false, "HS pings every 15 s, so a live stream never goes 35 s quiet");
  assert.equal(stalled(0, 35_000), true);
});

test("a missed offer leaves a notice until it is dismissed", () => {
  let s = init(me as any, [], []);
  s = reduce(s, { type: "event", e: ev("offer.created", { offer_id: "o1", channel: "call", from: "+14155550142", via: "queue" }) });
  s = reduce(s, { type: "event", e: ev("offer.missed", { offer_id: "o1", strike: 2 }) });
  assert.deepEqual(s.notices.map((n) => [n.missed, n.from, n.strike]), [[true, "+14155550142", 2]]);
  s = reduce(s, { type: "dismiss", id: s.notices[0].id });
  assert.equal(s.notices.length, 0);
});

test("a task holds a line from its accept, a callback's call takes the call card, and a warm transfer target reads as consulting", () => {
  const s = init(me as any, [], []);
  assert.equal(reduce(s, { type: "line", cid: "c1", conv: conv("active", "primary", "task") }).lines[0].phase, "live");
  const calling = reduce(s, { type: "line", cid: "c1", conv: conv("active", "primary", "callback", { via: "outbound" }) });
  assert.equal(stage(calling).kind, "live", "the call a callback places shows in the call card");
  const task = reduce(s, { type: "line", cid: "c1", conv: conv("wrap_up", "primary", "task") });
  assert.equal(task.lines[0].phase, "wrap");
  const target = reduce(s, { type: "line", cid: "c1", conv: conv("active", "secondary", "call", { via: "transfer", mode: "warm" }) });
  assert.equal(target.lines[0].consult, true);
});

test("a callback's line comes back from its call off hold, unmuted and recording", () => {
  let s = reduce(init(me as any, [], []), { type: "line", cid: "c1", conv: conv("active", "primary", "callback", { via: "outbound" }) });
  s = reduce(s, { type: "event", e: ev("call.held", { participant_id: null }) });
  s = reduce(s, { type: "event", e: ev("call.recording_paused", {}) });
  s = reduce(s, { type: "patch", cid: "c1", patch: { muted: true } });
  s = reduce(s, { type: "event", e: ev("call.ended", {}) });
  s = reduce(s, { type: "line", cid: "c1", conv: conv("active", "primary", "callback") });
  const { held, muted, paused } = s.lines[0];
  assert.deepEqual({ held, muted, paused }, { held: false, muted: false, paused: false }, "the next call starts clean");
  assert.equal(stage(s).kind, "idle", "the callback outlives its call without a call card");
});

test("only a call whose wrap-up event says auto counts down to an auto wrap-up", () => {
  const auto = { ...me, settings: { agent_direct_dial: false, wrap_up: { mode: "auto" } } };
  const wrap = (mode: string | null, channel = "call") => {
    let s = reduce(init(auto as any, [], []), { type: "line", cid: "c1", conv: conv("wrap_up", "primary", channel) });
    if (mode) s = reduce(s, { type: "event", e: { type: "conversation.wrap_up_started", conversation_id: "c1", data: { mode } } as any });
    return autoWrap(s, s.lines[0]);
  };
  assert.equal(wrap("auto"), true);
  assert.equal(wrap("manual"), false);
  assert.equal(wrap(null), false);
  assert.equal(wrap("auto", "task"), false);
});

test("an SMS rings as an offer, then holds a line beside a call, and each line keeps to itself", () => {
  let s = reduce(init(me as any, [], []), { type: "line", cid: "c1", conv: conv("active") });
  s = reduce(s, { type: "event", e: ev("offer.created", { offer_id: "o2", channel: "sms", from: "+14155550199", via: "queue", ring_timeout_sec: 60 }, { conversation_id: "c2" }) });
  assert.equal(pending(s)[0]?.channel, "sms", "a text offer shows beside the live call");
  assert.equal(sdkFree(s), false, "the live call still holds the phone");
  s = reduce(s, { type: "line", cid: "c2", conv: conv("active", "primary", "sms", {}, "c2") });
  assert.deepEqual(s.lines.map((l) => l.cid), ["c1", "c2"]);
  assert.equal(pending(s).length, 0, "the accepted offer leaves the ringing list");
  assert.equal(voice(s)?.cid, "c1", "the call card keeps the call while a text is open");
  assert.equal(s.focus, "c2/live", "the new line takes the screen");
  s = reduce(s, { type: "event", e: ev("call.held", { participant_id: null }) });
  assert.deepEqual(s.lines.map((l) => l.held), [true, false], "a call's hold changes only the call");
  s = reduce(s, { type: "line", cid: "c2", conv: conv("wrap_up", "primary", "sms", {}, "c2") });
  assert.equal(s.focus, "c2/wrap", "a line that reaches wrap-up takes the screen again");
  s = reduce(s, { type: "line", cid: "c2", conv: null });
  assert.deepEqual(s.lines.map((l) => l.cid), ["c1"], "a closed text leaves only its own line");
});

test("a desk that reloads keeps the text and task offers /me still lists", () => {
  const offered = { offer_id: "o3", conversation_id: "c3", from: "+14155550199", via: "queue", offered_at: "2026-11-10T10:00:00Z", expires_at: "2026-11-10T10:01:00Z" };
  const s = init({ ...me, pending_offers: [{ ...offered, channel: "whatsapp" }, { ...offered, offer_id: "o4", channel: "task" }] } as any, [], []);
  assert.deepEqual(s.offers.map((o) => [o.id, o.channel, o.ringFor]), [["o3", "whatsapp", 60], ["o4", "task", 60]]);
});

const msg = (id: string, direction: "inbound" | "outbound", created_at: string, extra: object = {}) => ({ id, direction, text: id, attachments: [], email: null, status: direction === "inbound" ? "received" : "delivered", created_at, ...extra });

test("a thread reads messages and the line's events in time order, with each message's side", () => {
  const msgs = [msg("hi", "inbound", "2026-11-10T10:00:00Z"), msg("hello", "outbound", "2026-11-10T10:02:00Z")];
  const feed = [ev("offer.accepted", {}, { id: "e2", created_at: "2026-11-10T10:01:00Z" }), ev("message.sent", {}, { id: "e3", created_at: "2026-11-10T10:02:00Z" })];
  const rows = thread(msgs as any, feed as any, (e) => e.type);
  assert.deepEqual(rows.map((r) => [r.text, r.side, r.note]), [["hi", "inbound", undefined], ["offer.accepted", undefined, undefined], ["hello", "outbound", "delivered"]], "a message event shows as its message, not twice");
});

test("a call's notes show in its thread in time order, each once", () => {
  const feed = [ev("call.answered", {}, { id: "e1", created_at: "2026-11-10T10:00:00Z" }), ev("note.added", { text: "refund" }, { id: "e2", created_at: "2026-11-10T10:01:00Z" })];
  const notes = [{ id: "n1", text: "refund", agent_id: "me", created_at: "2026-11-10T10:01:00Z" }];
  assert.deepEqual(thread([], feed as any, (e) => e.type, notes).map((r) => [r.text, r.side]), [["call.answered", undefined], ["refund", "note"]], "a note event shows as its note, not twice");
});

test("WhatsApp takes a free reply only within 24 hours of the customer's last message", () => {
  const now = Date.parse("2026-11-10T12:00:00Z");
  assert.equal(windowShut("whatsapp", [msg("a", "inbound", "2026-11-09T13:00:00Z")] as any, now), false);
  assert.equal(windowShut("whatsapp", [msg("a", "inbound", "2026-11-09T11:00:00Z"), msg("b", "outbound", "2026-11-10T11:00:00Z")] as any, now), true, "an agent's message does not reopen it");
  assert.equal(windowShut("whatsapp", [], now), true, "a WhatsApp the customer never wrote in is shut");
  assert.equal(windowShut("sms", [], now), false);
});

test("an email reply keeps the customer's subject", () => {
  assert.equal(replySubject([msg("a", "inbound", "2026-11-10T10:00:00Z", { email: { subject: "Order 12" } })] as any), "Re: Order 12");
  assert.equal(replySubject([msg("a", "inbound", "2026-11-10T10:00:00Z", { email: { subject: "RE: Order 12" } })] as any), "RE: Order 12");
  assert.equal(replySubject([]), undefined);
});

test("the dialer's recent calls are the last five numbers called, newest first, each once, named when a contact has the number", async () => {
  const calls = ["+1", "+2", "+1", "+3", "+4", "+5", "+6"].map((number, i) => ({ id: `c${i}`, number }));
  const rows = recent(calls as any, [{ name: "Billing desk", number: "+2" }]);
  assert.deepEqual(rows.map((c) => [c.number, c.name]), [["+1", undefined], ["+2", "Billing desk"], ["+3", undefined], ["+4", undefined], ["+5", undefined]]);
  const asked: unknown[][] = [];
  await recentCalls(async (...a) => (asked.push(a), { data: [] }), "a1");
  assert.deepEqual(asked, [["GET", "/api/v1/conversations?agent_id=a1&direction=outbound&channel=call&limit=20"]], "only this agent's outbound calls are read");
});

test("a note on a call is posted to that call's notes", async () => {
  const asked: unknown[][] = [];
  const cid = "c1";
  await note(async (...a) => asked.push(a), cid, "Wants a refund");
  assert.deepEqual(asked, [["POST", `/api/v1/conversations/${cid}/notes`, { text: "Wants a refund" }]]);
});

test("the task sheet asks each kind for what the API's validator needs of it, and an optional due date", () => {
  for (const kind of ["internal", "others", "task"] as const) assert.deepEqual(asks(kind, true), { due_at: false }, `${kind} takes no schedule and no channel`);
  assert.deepEqual(asks("callback", false), { due_at: false, customer: true }, "a callback about no conversation needs the customer's number");
  assert.deepEqual(asks("callback", true), { due_at: false, customer: false }, "one raised from a conversation may take that conversation's customer");
  assert.deepEqual(asks("follow_up", true), { channel: true, scheduled_at: true }, "an agent's follow-up needs its channel and a schedule, and is due then");
});

test("the task sheet sends one POST /tasks, with only the fields its kind takes", async () => {
  const sent: unknown[][] = [];
  const ask = async (...a: unknown[]) => void sent.push(JSON.parse(JSON.stringify(a)));
  const form = { title: "Refund", description: "", priority: "high", due_at: "2030-01-03T17:00", scheduled_at: "2030-01-02T09:30", customer: "", channel: "sms", target: "agent:a1" };
  const [due, at] = [new Date("2030-01-03T17:00").toISOString(), new Date("2030-01-02T09:30").toISOString()];
  await createTask(ask, { ...form, type: "callback" }, "c1");
  await createTask(ask, { ...form, type: "callback", customer: "+14155550142" });
  await createTask(ask, { ...form, type: "follow_up", target: "team:t1" }, "c1");
  await createTask(ask, { ...form, type: "internal", description: "Check the order" });
  await createTask(ask, { ...form, type: "others", due_at: "" }, "c1");
  await createTask(ask, { ...form, type: "task", due_at: "2020-01-03T17:00" });
  const common = { title: "Refund", priority: "high", target: { type: "agent", agent_id: "a1" } };
  assert.deepEqual(sent, [
    ["POST", "/api/v1/tasks", { type: "callback", ...common, conversation_id: "c1", due_at: due }],
    ["POST", "/api/v1/tasks", { type: "callback", ...common, customer: "+14155550142", due_at: due }],
    ["POST", "/api/v1/tasks", { type: "follow_up", ...common, target: { type: "team", team_id: "t1" }, conversation_id: "c1", channel: "sms", scheduled_at: at, due_at: at }],
    ["POST", "/api/v1/tasks", { type: "internal", ...common, description: "Check the order", due_at: due }],
    ["POST", "/api/v1/tasks", { type: "others", ...common, conversation_id: "c1" }],
    ["POST", "/api/v1/tasks", { type: "task", ...common, due_at: new Date("2020-01-03T17:00").toISOString() }],
  ]);
});

test("a task's line shows its own task, not a newer one raised from it", () => {
  const read = [{ conversation_id: "c9", title: "Raised from it" }, { conversation_id: "c1", title: "Call back" }];
  assert.equal(own(read, "c1")?.title, "Call back");
  assert.equal(own(read.slice(0, 1), "c1"), undefined);
});

test("a hangup the service refuses because the call is over hangs up this browser's leg and clears the call", async () => {
  const done: string[] = [];
  const phone = { hangup: () => done.push("leg hung up") };
  const refused = (code: string) => () => Promise.reject(Object.assign(new Error("the call is not live"), { code }));
  await endCall(refused("call_not_live"), phone, () => done.push("call cleared"));
  assert.deepEqual(done, ["leg hung up", "call cleared"]);
  await assert.rejects(endCall(refused("forbidden"), phone, () => done.push("call cleared")), /not live/, "any other refusal is shown");
  await endCall(() => Promise.resolve(), phone, () => done.push("call cleared"));
  assert.equal(done.length, 2, "a hangup the service takes leaves the rest to its events");
});
