import assert from "node:assert/strict";
import { test } from "node:test";
import { start } from "./start.ts";

function fakeHs(fail = "") {
  const hs = { agents: [] as any[], queues: [] as any[], dispositions: [{ name: "No answer", system: true }] as any[], numbers: {} as Record<string, unknown>, settings: {}, sent: [] as string[] };
  const call = async (method: string, path: string, body: any): Promise<any> => {
    if (`${method} ${path}` === fail) throw ((fail = ""), new Error("HS did not answer"));
    hs.sent.push(`${method} ${path}`);
    if (path === "/api/v1/agents") return hs.agents[hs.agents.push({ id: "a1", ...body }) - 1];
    if (path === "/api/v1/queues") return hs.queues[hs.queues.push({ id: "q1", ...body }) - 1];
    if (path.startsWith("/api/v1/queues/")) return Object.assign(hs.queues[0], body);
    if (path === "/api/v1/dispositions") return hs.dispositions.push(body);
    if (path.startsWith("/api/v1/numbers/") && method === "GET")
      return hs.numbers[decodeURIComponent(path.slice(16))] ?? Promise.reject(Object.assign(new Error("no such number"), { status: 404 }));
    if (path.startsWith("/api/v1/numbers/")) return (hs.numbers[decodeURIComponent(path.slice(16))] = body);
    if (path === "/api/v1/settings") return Object.assign(hs.settings, body);
    return { agent_token: "t" };
  };
  return { hs, api: { call, agents: async () => hs.agents, queues: async () => hs.queues, dispositions: async () => hs.dispositions } };
}

test("get started converges: run again after a failed step, or twice, it adds one agent and one queue", async () => {
  const { hs, api } = fakeHs("PUT /api/v1/numbers/%2B14155550100");
  await assert.rejects(start(api, "Ada", "+14155550100"), /^Error: Routing \+14155550100 to General: HS did not answer$/);
  await start(api, "Ada", "+14155550100");
  await start(api, "Ada", "+14155550100");
  assert.equal(hs.agents.length, 1);
  assert.deepEqual(hs.queues, [{ id: "q1", name: "General", agents: ["a1"] }]);
  assert.deepEqual(hs.dispositions.map((d) => d.name), ["No answer", "Resolved", "Follow-up needed", "Not resolved"], "a system disposition is not one an agent picks");
  assert.deepEqual(hs.numbers, { "+14155550100": { open_action: { type: "queue", queue_id: "q1" }, business_hours_id: null, closed_action: null } });
  assert.deepEqual(hs.settings, { agent_direct_dial: true, default_from: { call: "+14155550100" } });
  assert.deepEqual(hs.sent.slice(-2), ["PATCH /api/v1/settings", "POST /api/v1/agents/a1/sessions"], "the session's phone token is minted once calling out is on");
});

test("an existing General queue gains you, and outcomes of the organization's own stay alone", async () => {
  const { hs, api } = fakeHs();
  hs.queues.push({ id: "q0", name: "General", agents: ["b"] });
  hs.dispositions.push({ name: "Sale", system: false, parent_disposition_id: null });
  await start(api, "Ada", "+14155550100");
  assert.deepEqual(hs.queues, [{ id: "q0", name: "General", agents: ["b", "a1"] }]);
  assert.equal(hs.dispositions.length, 2);
});

test("without a number, only you and your session are added", async () => {
  const { hs, api } = fakeHs();
  await start(api, "Ada");
  assert.deepEqual(hs.sent, ["POST /api/v1/agents", "POST /api/v1/agents/a1/sessions"]);
});

test("your email and password are set before your session, which a new password would end", async () => {
  const { hs, api } = fakeHs();
  await start(api, "Ada", "+14155550100", { email: "ada@example.com", password: "correct horse" });
  assert.equal(hs.agents[0].email, "ada@example.com");
  assert.deepEqual(hs.sent.slice(0, 2), ["POST /api/v1/agents", "POST /api/v1/agents/a1/password"]);
  assert.equal(hs.sent.at(-1), "POST /api/v1/agents/a1/sessions");
});

test("routing a number that has hours keeps its hours and its after-hours action", async () => {
  const { hs, api } = fakeHs();
  hs.numbers["+14155550100"] = { number: "+14155550100", open_action: { type: "queue", queue_id: "old" }, business_hours_id: "h1", closed_action: { type: "voicemail", mailbox: { queue_id: "old" } } };
  await start(api, "Ada", "+14155550100");
  assert.deepEqual(hs.numbers["+14155550100"], { open_action: { type: "queue", queue_id: "q1" }, business_hours_id: "h1", closed_action: { type: "voicemail", mailbox: { queue_id: "old" } } });
});
