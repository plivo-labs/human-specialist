import assert from "node:assert/strict";
import { test } from "node:test";
import { holds, landing, renewDue, signInTrouble, visible } from "./held.ts";

const NAV = [{ route: "/desk", need: "session" as const }, { route: "/floor", need: "boss" as const }, { route: "/agents" }];
const shown = (key: boolean, role?: string | null) => visible(NAV, holds(key, role)).map((n) => n.route);

test("the nav shows what the browser's key and session open", () => {
  assert.deepEqual(shown(true), ["/agents"], "the key alone opens setup");
  assert.deepEqual(shown(false, "agent"), ["/desk"], "an agent's session opens only the desk");
  assert.deepEqual(shown(false, null), ["/desk"], "a session with no role is an agent's");
  assert.deepEqual(shown(false, "supervisor"), ["/desk", "/floor"]);
  assert.deepEqual(shown(false, "admin"), ["/desk", "/floor", "/agents"], "an admin's own session opens setup without the key");
  assert.deepEqual(shown(true, "agent"), ["/desk", "/agents"]);
  assert.equal(landing(holds(true)), "/agents");
  assert.equal(landing(holds(false, "admin")), "/desk");
});

test("a session renews six minutes before it ends, then once a minute until it works", () => {
  const ends = 60 * 60e3;
  assert.equal(renewDue(ends, ends - 7 * 60e3, 0), false);
  assert.equal(renewDue(ends, ends - 6 * 60e3, 0), true);
  assert.equal(renewDue(ends, ends - 5.5 * 60e3, ends - 6 * 60e3), false, "a failed try waits a minute");
  assert.equal(renewDue(ends, ends - 5 * 60e3, ends - 6 * 60e3), true);
  assert.equal(renewDue(ends, ends + 60e3, 0), true, "a tab that slept past the end renews when it wakes");
  assert.equal(renewDue(ends, ends - 60e3, Infinity), false, "a chain past its 12 hours stops renewing");
});

test("a sign-in that fails tells a user with no password what to do", () => {
  assert.match(signInTrouble("invalid_credentials", undefined)!, /administrator.*Settings, Users/);
});

test("a sign-in says what a 409 and a 429 need", () => {
  assert.match(signInTrouble("tenant_required", undefined)!, /sign-in link/);
  assert.equal(signInTrouble("too_many_attempts", 840), "Too many tries. Try again in 14 minutes.");
  assert.equal(signInTrouble("too_many_attempts", 30), "Too many tries. Try again in a minute.");
});
