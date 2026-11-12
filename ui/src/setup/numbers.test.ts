import assert from "node:assert/strict";
import { test } from "node:test";
import { pickable, route, routedBy, shownOf } from "./numbers.ts";

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
