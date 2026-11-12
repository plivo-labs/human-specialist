import assert from "node:assert/strict";
import { test } from "node:test";
import { explain, operatorKey, time } from "./format.ts";

test("a 422 names each field and why it was refused", () => {
  const details = {
    fields: [
      { loc: ["body", "time_zone"], msg: "Value error, not an IANA time zone" },
      { loc: ["body", "slots", 0], msg: "Value error, start must be before end" },
      { loc: ["body"], msg: "Value error, min_level is below max_level" },
    ],
  };
  assert.equal(explain("the request is invalid", details), "the request is invalid. Time zone: not an IANA time zone. Slots: start must be before end. Min level is below max level");
  assert.equal(explain("A queue has this name.", undefined), "A queue has this name");
  assert.equal(explain("a number or menu names this", { used_by: [{ kind: "number", number: "+14155550100" }, { kind: "menu" }] }), "a number or menu names this. Used by +1 415 555 0100, a menu");
  assert.equal(explain("conversations are waiting in this queue", { waiting: 2 }), "conversations are waiting in this queue. 2 waiting. Disable the queue, and delete it once they are taken or closed");
  assert.equal(explain("conversations are waiting in this queue or scheduled for it", { scheduled: 1 }), "conversations are waiting in this queue or scheduled for it. 1 scheduled for it. Give them another target or cancel them");
  assert.equal(explain("Plivo refused the request", { status: 402, errors: { error: "Insufficient balance" } }), "Plivo refused the request. Insufficient balance (402)");
});

test("a time from another day carries its date", () => {
  assert.match(time(new Date().toISOString()), /^\d{1,2}:\d\d [AP]M$/);
  assert.match(time(new Date(Date.now() - 2 * 86400e3).toISOString()), /^[A-Z][a-z]{2} \d{1,2}, \d{1,2}:\d\d [AP]M$/);
});

test("the operator key is accepted bare or as the line docker/.env holds", () => {
  assert.equal(operatorKey(" HS_OPERATOR_KEY=abc= "), "abc=");
  assert.equal(operatorKey("abc"), "abc");
});
