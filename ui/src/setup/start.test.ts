import assert from "node:assert/strict";
import { test } from "node:test";
import { nameOf, registrar, start } from "./start.ts";

function fakeHs(fail = "") {
  const hs = { agents: [] as any[], queues: [] as any[], dispositions: [{ name: "No answer", system: true }] as any[], sent: [] as string[] };
  const call = async (method: string, path: string, body: any): Promise<any> => {
    if (`${method} ${path}` === fail) throw ((fail = ""), new Error("HS did not answer"));
    hs.sent.push(`${method} ${path}`);
    if (path === "/api/v1/agents") return hs.agents[hs.agents.push({ id: "a1", ...body }) - 1];
    if (path === "/api/v1/queues") return hs.queues[hs.queues.push({ id: "q1", ...body }) - 1];
    if (path.startsWith("/api/v1/queues/")) return Object.assign(hs.queues[0], body);
    if (path === "/api/v1/dispositions") return hs.dispositions.push(body);
    return { agent_token: "t" };
  };
  return { hs, api: { call, agents: async () => hs.agents, queues: async () => hs.queues, dispositions: async () => hs.dispositions } };
}

const ada = { name: "Ada", email: "ada@example.com", password: "correct horse" };

test("registering converges: run again after a failed step, or twice, it adds one admin, one General queue and the outcomes, and no number", async () => {
  const { hs, api } = fakeHs("POST /api/v1/dispositions");
  await assert.rejects(start(api, ada, "register"), /^Error: Adding the disposition Resolved: HS did not answer$/);
  await start(api, ada, "register");
  await start(api, ada, "register");
  assert.deepEqual(hs.agents, [{ id: "a1", name: "Ada", role: "admin", email: "ada@example.com" }]);
  assert.deepEqual(hs.queues, [{ id: "q1", name: "General", agents: ["a1"] }]);
  assert.deepEqual(hs.dispositions.map((d) => d.name), ["No answer", "Resolved", "Follow-up needed", "Not resolved"], "a system disposition is not one an agent picks");
  assert.ok(hs.sent.every((s) => !/numbers|settings/.test(s)), "the phone number is a later step");
  assert.deepEqual(hs.sent.slice(-2), ["POST /api/v1/agents/a1/password", "POST /api/v1/agents/a1/sessions"], "a new password would end an earlier session");
});

test("an existing General queue gains you, and outcomes of the organization's own stay alone", async () => {
  const { hs, api } = fakeHs();
  hs.queues.push({ id: "q0", name: "General", agents: ["b"] });
  hs.dispositions.push({ name: "Sale", system: false, parent_disposition_id: null });
  await start(api, ada, "register");
  assert.deepEqual(hs.queues, [{ id: "q0", name: "General", agents: ["b", "a1"] }]);
  assert.equal(hs.dispositions.length, 2);
});

test("adding yourself to an organization adds only you, your password and your session", async () => {
  const { hs, api } = fakeHs();
  await start(api, ada);
  assert.deepEqual(hs.sent, ["POST /api/v1/agents", "POST /api/v1/agents/a1/password", "POST /api/v1/agents/a1/sessions"]);
});

test("Who are you? leaves an administrator with your email alone until you ask to set a new password", async () => {
  const { hs, api } = fakeHs();
  hs.agents.push({ id: "a0", name: "Ada", role: "admin", email: "someone@example.com" }, { id: "a9", name: "A. Lovelace", role: "admin", email: "ADA@example.com" });
  assert.deepEqual(await start(api, ada), { me: hs.agents[1], session: null });
  assert.deepEqual(hs.sent, [], "no password set and no session replaced");
  await start(api, ada, "reset");
  assert.deepEqual(hs.sent, ["POST /api/v1/agents/a9/password", "POST /api/v1/agents/a9/sessions"], "the admin with your email in any case, not one who shares your name");
});

test("you sign in with the email HS holds for you, as it holds it", async () => {
  const { hs, api } = fakeHs();
  hs.agents.push({ id: "a9", name: "Ada", role: "admin", email: "ADA@example.com" });
  assert.equal((await start(api, ada, "register")).me.email, "ADA@example.com");
  assert.equal((await start(fakeHs().api, ada, "register")).me.email, "ada@example.com");
});

test("a registration names you after your email", () => {
  assert.equal(nameOf("ada.lovelace@example.com"), "Ada Lovelace");
  assert.equal(nameOf("grace_hopper-1@example.com"), "Grace Hopper 1");
  assert.equal(nameOf("x@example.com"), "X");
});

function registering(fail = "") {
  const { hs, api } = fakeHs(fail);
  const log: string[] = [];
  let made = 0;
  const reg = registrar(
    async (plivo, operator) => (log.push(`create ${plivo.plivo_auth_id} ${operator}`), { tenant_id: `t${++made}`, api_key: `k${made}` }),
    (key) => (log.push(`as ${key}`), api),
    () => log.push("forget"),
  );
  return { hs, log, reg };
}
const filled = () => {
  const f = new FormData();
  for (const [k, v] of Object.entries({ auth_id: "MA1", auth_token: "token", email: "ada@example.com", password: "correct horse" })) f.set(k, v);
  return f;
};

test("after the organization exists, Try again submits the frozen form with no operator key and creates nothing twice", async () => {
  const { hs, log, reg } = registering("POST /api/v1/dispositions");
  await assert.rejects(reg.submit(filled(), "op"), /^Error: Adding the disposition Resolved: HS did not answer$/);
  assert.equal(reg.pending(), true, "the organization and its key wait in memory");
  const before = hs.sent.length;
  const done = await reg.submit(new FormData(), "");
  assert.deepEqual(log, ["forget", "create MA1 op", "as k1", "forget", "as k1"], "each press forgets the earlier session first, and the organization is made once");
  const again = ["POST /api/v1/agents/a1/password", ...Array(3).fill("POST /api/v1/dispositions"), "POST /api/v1/agents/a1/sessions"];
  assert.deepEqual(hs.sent.slice(before), again, "no second administrator or General queue, only the outcomes still missing");
  assert.deepEqual([done.tenant_id, done.api_key, done.me.email], ["t1", "k1", "ada@example.com"], "the frozen email, not the empty form's");
  assert.equal(reg.pending(), false, "nothing of the registration stays once you are signed in");
});

test("a second press while a registration runs makes no second organization", async () => {
  const { log, reg } = registering();
  const [a, b] = await Promise.all([reg.submit(filled(), ""), reg.submit(filled(), "")]);
  assert.equal(a, b);
  assert.deepEqual(log.filter((l) => l.startsWith("create")), ["create MA1 "]);
});

test("a create request that gets no answer says the organization may exist and how to finish it", async () => {
  for (const status of [0, 502, 504]) {
    const reg = registrar(() => Promise.reject(Object.assign(new Error("no answer"), { status })), () => fakeHs().api, () => {});
    await assert.rejects(reg.submit(filled(), ""), /may have been created\. .*Make a new organization key\.$/);
    assert.equal(reg.pending(), false);
  }
  const reg = registrar(() => Promise.reject(Object.assign(new Error("wrong token"), { status: 401 })), () => fakeHs().api, () => {});
  await assert.rejects(reg.submit(filled(), ""), /^Error: wrong token$/, "an answer is shown as it came");
});
