import assert from "node:assert/strict";
import { test } from "node:test";
import { callersTo, pickable, route, routeTo, routedBy, reach, saveQueue, saveUser, sendUser, shownOf, textWait, userBody } from "./numbers.ts";

const row = { id: "n1", number: "+14155550100", open_action: { type: "queue", queue_id: "q1", hold_position: true }, business_hours_id: "h1", closed_action: { type: "hangup", greeting: { text: "Closed" } } };

test("a number's PUT changes only what the user picked", () => {
  const shown = shownOf(row);
  assert.deepEqual(route({ ...shown, open: "queue:q2" }, shown, row), {
    open_action: { type: "queue", queue_id: "q2", hold_position: true },
    business_hours_id: "h1",
    closed_action: { type: "hangup", greeting: { text: "Closed" } },
  }, "re-routing keeps the hours and the after-hours action");
  assert.deepEqual(route({ ...shown, hours: "" }, shown, row).open_action, row.open_action);
  assert.equal(route({ ...shown, hours: "" }, shown, row).business_hours_id, null);
});

test("adding a number already on the list keeps what the sheet left alone", () => {
  const blank = shownOf();
  assert.deepEqual(route({ open: "queue:q2", hours: "", closed: "hangup" }, blank, row), {
    open_action: { type: "queue", queue_id: "q2", hold_position: true },
    business_hours_id: "h1",
    closed_action: row.closed_action,
  });
});

test("a new number's after-hours action", () => {
  const blank = shownOf();
  assert.equal(route({ open: "queue:q1", hours: "h1", closed: "hangup" }, blank).closed_action, null);
  assert.deepEqual(route({ open: "queue:q1", hours: "h1", closed: "voicemail" }, blank).closed_action, { type: "voicemail", mailbox: { queue_id: "q1" } });
  assert.deepEqual(route({ open: "queue:q1", hours: "h1", closed: "queue:q3" }, blank).closed_action, { type: "queue", queue_id: "q3" });
});

test("a queue is routed to by what names it, and a disabled one is offered only where chosen", () => {
  const from: [string, { id: string }][] = [["+1 415 555 0100", row], ["menu Main", { id: "m1", items: [{ queue_id: "q1" }] }], ["queue Overflow", { id: "q2", overflow_action: { type: "queue", queue_id: "q1" } }], ["queue General", { id: "q1", overflow_action: { queue_id: "q1" } }]];
  assert.deepEqual(routedBy("q1", from), ["+1 415 555 0100", "menu Main", "queue Overflow"]);
  assert.deepEqual(routedBy("q9", from), []);
  const queues = [{ id: "q1", enabled: true }, { id: "q2", enabled: false }, { id: "q3", enabled: false }];
  assert.deepEqual(pickable(queues, "queue:q2").map((q) => q.id), ["q1", "q2"]);
  assert.deepEqual(pickable(queues, "q3").map((q) => q.id), ["q1", "q3"]);
});

test("a caller ID routed from a user's sheet rings the user's queue, else the first enabled one, else hangs up", () => {
  const queues = [{ id: "q1", enabled: false, agents: ["a1"] }, { id: "q2", enabled: true, agents: [] }, { id: "q3", enabled: true, agents: ["a1"] }];
  assert.equal(callersTo(queues, "a1")?.id, "q3", "an enabled queue the user answers");
  assert.equal(callersTo(queues, "a9")?.id, "q2");
  assert.equal(callersTo(queues)?.id, "q2", "a user not saved yet");
  assert.deepEqual(routeTo(callersTo(queues, "a1")), { open_action: { type: "queue", queue_id: "q3" }, business_hours_id: null, closed_action: null });
  assert.deepEqual(routeTo(callersTo([])).open_action, { type: "hangup" });
});

test("the user form routes a new caller ID once: a failed save leaves it routed, and Save again sends only the user", async () => {
  const sent: string[] = [];
  const form = { listed: [{ number: "+14155550111", routed: false }], routed: new Set<string>(), users: [{ id: "a1", name: "Ann" }] };
  const route = async (n: string) => void sent.push(`PUT ${n}`);
  const refused = async () => {
    sent.push("PATCH user");
    throw new Error("The name is too long.");
  };
  await assert.rejects(saveUser(form, "+14155550111", route, refused), { message: "+14155550111 now routes to this organization, but the user was not saved. The name is too long." });
  await assert.rejects(saveUser(form, "+14155550111", route, refused), { message: "The name is too long." });
  assert.deepEqual(sent, ["PUT +14155550111", "PATCH user", "PATCH user"], "routing goes first, and Save again sends only the user");

  sent.length = 0;
  await assert.rejects(saveUser({ ...form, routed: new Set() }, "+14155550111", () => Promise.reject(new Error("Plivo refused")), refused), /Plivo refused/);
  assert.deepEqual(sent, [], "a routing that fails saves nothing");

  const taken = () => Promise.reject(Object.assign(new Error("exists"), { code: "agent_exists", details: { id: "a1" } }));
  await assert.rejects(saveUser(form, null, route, taken), { field: "email", message: "Ann already signs in with this email. Each user needs their own." }, "a taken email shows on the email field");
});

const caller = (answer: (line: string) => unknown = () => null) => {
  const sent: [string, unknown][] = [];
  return { sent, call: async <T>(method: string, path: string, body?: unknown) => (sent.push([`${method} ${path}`, body]), answer(`${method} ${path}`) as T) };
};
const form = (fields: [string, string][]) => fields.reduce((f, [k, v]) => (f.append(k, v), f), new FormData());
const sheet = (...set: [string, string][]) => {
  const f = form([["name", "Billing"], ["priority", "4"], ["capacity", "30"], ["agents", "a2"], ["agents", "a1"], ["wait_call", "20"], ["wait_chat", "300"], ["wait_sms", "0"], ["wait_whatsapp", "0"], ["wait_email", "0"], ["wait_callback", "0"], ["wait_task", "0"]]);
  set.forEach(([k, v]) => f.set(k, v));
  return f;
};

test("a queue's sheet sends only what it changed, and merges a changed wait into the queue's text as it is now", async () => {
  const q = { id: "q1", name: "Billing", enqueue_url: "", priority: 4, capacity: 30, enabled: true, agents: ["a1", "a2"], teams: [], skills: [], wait_timeout_sec: 20, text: { wait_timeout_sec: { chat: 300 } } };
  const now = { ...q, text: { wait_timeout_sec: { chat: 300, sms: 120 }, messages: { sms: { joined: "Hi" } } } };
  const { sent, call } = caller((line) => (line === "GET /api/v1/queues/q1" ? now : null));
  await saveQueue(call, sheet(["name", "Support"]), q);
  await saveQueue(call, sheet(["wait_chat", "600"]), q);
  await saveQueue(call, sheet(["wait_call", "90"]));
  assert.deepEqual(sent, [
    ["PATCH /api/v1/queues/q1", { name: "Support" }],
    ["GET /api/v1/queues/q1", undefined],
    ["PATCH /api/v1/queues/q1", { text: { wait_timeout_sec: { chat: 600, sms: 120, whatsapp: 0, email: 0, callback: 0, task: 0 }, messages: { sms: { joined: "Hi" } } } }],
    ["POST /api/v1/queues", { name: "Billing", priority: 4, capacity: 30, agents: ["a2", "a1"], teams: [], skills: [], wait_timeout_sec: 90, text: { wait_timeout_sec: { chat: 300, sms: 0, whatsapp: 0, email: 0, callback: 0, task: 0 } } }],
  ], "an untouched field, the same agents in another order, and the waits another admin changed since are left alone");
  assert.equal(textWait({ ...q, text: { wait_timeout_sec: 300 } }, "sms"), 300, "one wait for every channel shows in each");
  assert.deepEqual([textWait(q, "chat"), textWait(q, "sms"), textWait(undefined, "sms")], [300, 0, 0], "a channel left out waits with no limit");
});

test("a queue made through the API keeps its teams and skill rules when the console edits only its name", async () => {
  const rules = [{ skill_id: "s1", operator: ">=", level: 5 }, { skill_id: "s2", operator: "", level: 0 }, { skill_id: "s3", operator: "<>", level: 7 }];
  const q = { id: "q1", name: "Billing", enqueue_url: "", priority: 4, capacity: 30, enabled: true, agents: ["a1", "a2"], teams: ["t2", "t1"], skills: rules, wait_timeout_sec: 20, text: { wait_timeout_sec: { chat: 300 } } };
  const editor = (shown: typeof rules, ...set: [string, string][]) => {
    const f = sheet(...set);
    for (const t of ["t1", "t2"]) f.append("teams", t);
    for (const r of shown) [["skills", r.skill_id], ["operator", r.operator], ["level", String(r.level)]].forEach(([k, v]) => f.append(k, v));
    return f;
  };
  const { sent, call } = caller();
  await saveQueue(call, editor(rules, ["name", "Support"]), q);
  await saveQueue(call, editor([{ ...rules[0], level: 4 }, ...rules.slice(1)]), q);
  await saveQueue(call, sheet(), q);
  assert.deepEqual(sent, [
    ["PATCH /api/v1/queues/q1", { name: "Support" }],
    ["PATCH /api/v1/queues/q1", { skills: [{ skill_id: "s1", operator: ">=", level: 4 }, ...rules.slice(1)] }],
    ["PATCH /api/v1/queues/q1", { teams: [], skills: [] }],
  ], "the same teams in another order and the same rules are no change; a changed rule sends them all; unchecking and removing every one sends empty lists");
});

test("the queues table counts a queue's skills, not its rules", () => {
  const q = { id: "q1", name: "Billing", enqueue_url: "", priority: 4, capacity: 30, enabled: true };
  const rules = [{ skill_id: "s1", operator: ">=", level: 3 }, { skill_id: "s1", operator: "<=", level: 7 }];
  assert.equal(reach({ ...q, agents: ["a1", "a2"], teams: ["t1"], skills: rules }), "2 agents, 1 team, 1 skill");
  assert.equal(reach({ ...q, teams: ["t1", "t2"], skills: [...rules, { skill_id: "s2", operator: "", level: 0 }] }), "2 teams, 2 skills");
});

test("a queue's save shows an agent, team or skill deleted while the sheet was open on its field", async () => {
  const refusing = (kind: string) => caller(() => Promise.reject(Object.assign(new Error("refused"), { code: "foreign_id", details: { kind, ids: ["x1"] } }))).call;
  await assert.rejects(saveQueue(refusing("team"), sheet()), { field: "teams=x1", message: "This team was deleted. Uncheck it to save." });
  await assert.rejects(saveQueue(refusing("skill"), sheet()), { field: "skills=x1", message: "This skill was deleted. Remove its row to save." });
  await assert.rejects(saveQueue(refusing("agent"), sheet()), { field: "agents=x1" });
  await assert.rejects(saveQueue(refusing("queue"), sheet()), (e: object) => !("field" in e), "any other refusal stays a toast");
});

const cy = { id: "a1", name: "Cy", role: "agent" as const, email: "cy@acme.example", teams: ["t1"], skills: [{ skill_id: "s2", level: null }, { skill_id: "s1", level: 7 }], caller_id: null, status: null, endpoint_username: null };
const user = (...skills: [string, string][]): [string, string][] => [["name", "Cy"], ["role", "agent"], ["email", " cy@acme.example "], ["teams", "t1"], ...skills, ["caller_id", ""]];

test("a user's sheet sends only what it changed: the skills when they changed, an empty list when the last one goes", async () => {
  const { sent, call } = caller();
  await sendUser(call, userBody(form(user(["skills", "s1"], ["level_s1", "7"], ["skills", "s2"], ["level_s3", "4"]))), cy);
  await sendUser(call, { ...userBody(form(user(["skills", "s1"], ["level_s1", "7"], ["skills", "s2"]))), name: "Cy O" }, cy);
  await sendUser(call, userBody(form(user(["skills", "s1"], ["level_s1", "8"], ["skills", "s2"]))), cy);
  await sendUser(call, userBody(form(user())), cy);
  await sendUser(call, userBody(form([["name", "Di"], ["role", "agent"], ["email", ""], ["caller_id", ""]])));
  assert.deepEqual(sent, [
    ["PATCH /api/v1/agents/a1", {}],
    ["PATCH /api/v1/agents/a1", { name: "Cy O" }],
    ["PATCH /api/v1/agents/a1", { skills: [{ skill_id: "s1", level: 8 }, { skill_id: "s2", level: null }] }],
    ["PATCH /api/v1/agents/a1", { skills: [] }],
    ["POST /api/v1/agents", { name: "Di", role: "agent", email: null, teams: [], skills: [], caller_id: null }],
  ], "the same skills in another order, and an unchecked skill's level, are no change");
});

test("a level or a skill the service refuses shows on that skill's row in plain words", async () => {
  const refusing = (e: object) => caller(() => Promise.reject(Object.assign(new Error("refused"), e))).call;
  const b = userBody(form(user(["skills", "s1"], ["level_s1", "7"], ["skills", "s2"])));
  await assert.rejects(sendUser(refusing({ code: "skill_level_invalid", details: { skill_id: "s1" } }), b, cy), { field: "level_s1", message: "This skill's levels changed after the page loaded. Reload the page to see them." });
  await assert.rejects(sendUser(refusing({ code: "skill_level_invalid", details: { skill_id: "s2" } }), b), { field: "skill_s2" }, "a skill that now takes a level, sent without one, shows on its checkbox");
  await assert.rejects(sendUser(refusing({ code: "foreign_id", details: { kind: "skill", ids: ["s1"] } }), b), { field: "skill_s1", message: "This skill was deleted. Uncheck it to remove it." });
  await assert.rejects(sendUser(refusing({ code: "foreign_id", details: { kind: "team", ids: ["t9"] } }), b), (e: object) => !("field" in e), "any other refusal stays a toast");
});
