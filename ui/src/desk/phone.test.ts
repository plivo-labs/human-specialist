import assert from "node:assert/strict";
import { test } from "node:test";
import { askForMicrophone, carries, key, tone } from "./phone.ts";

const notes = new Map<string, string>();
Object.defineProperty(globalThis, "localStorage", {
  value: { getItem: (k: string) => notes.get(k) ?? null, setItem: (k: string, v: string) => notes.set(k, v), removeItem: (k: string) => notes.delete(k) },
  configurable: true,
});

function browser(state: string | null, answer: "allow" | "refuse" = "allow") {
  const opened: string[] = [];
  const permissions = state === null ? undefined : { query: async () => ({ state }) };
  const getUserMedia = async () => {
    opened.push("mic");
    if (answer === "refuse") throw Object.assign(new Error("refused"), { name: "NotAllowedError" });
    return { getTracks: () => [{ stop() {} }] };
  };
  Object.defineProperty(globalThis, "navigator", { value: { permissions, mediaDevices: { getUserMedia } }, configurable: true });
  return opened;
}

test("the desk opens the microphone on load only while the browser has not decided", async () => {
  notes.clear();
  let opened = browser("prompt");
  assert.equal(await askForMicrophone(), "", "an undecided browser is asked, then calls ring");
  assert.deepEqual(opened, ["mic"]);

  opened = browser("granted");
  assert.equal(await askForMicrophone(), "", "calls ring in a granted browser");
  assert.deepEqual(opened, [], "a granted microphone stays closed on load");

  opened = browser("denied");
  assert.match(await askForMicrophone(), /blocked the microphone/);
  assert.deepEqual(opened, [], "a denied microphone is not asked again");
});

test("without the Permissions API the desk asks once on this device", async () => {
  notes.clear();
  let opened = browser(null);
  assert.equal(await askForMicrophone(), "", "the first visit asks, then calls ring");
  assert.deepEqual(opened, ["mic"]);

  opened = browser(null);
  assert.equal(await askForMicrophone(), "", "calls ring on the next visit");
  assert.deepEqual(opened, [], "the next visit does not open the microphone");

  notes.clear();
  opened = browser(null, "refuse");
  assert.match(await askForMicrophone(), /blocked the microphone/);
  opened = browser(null);
  await askForMicrophone();
  assert.deepEqual(opened, ["mic"], "a refused ask leaves no note, so the next visit asks again");
});

test("a granted note does not outlast the browser's own answer", async () => {
  notes.clear();
  browser(null);
  await askForMicrophone();
  let opened = browser("prompt");
  await askForMicrophone();
  assert.deepEqual(opened, ["mic"], "a browser that says it has not decided is asked even with the note set, or the first ring would wait on its prompt and time out");
  browser("denied");
  await askForMicrophone();
  opened = browser(null);
  await askForMicrophone();
  assert.deepEqual(opened, ["mic"], "a denial clears the note");
});

test("the keypad sends the twelve keys of a phone and nothing else typed", () => {
  assert.deepEqual([..."0123456789*#"].map(key), [..."0123456789*#"]);
  assert.deepEqual(["", "a", "+", "12", "Enter", " "].map(key), [null, null, null, null, null, null], "an empty key is not a digit, though every string includes it");
});

test("the keypad and mute belong to the call this browser answered, and a key goes only while it is still connected", () => {
  const sent: string[] = [];
  const phone = (uuid: string | null, connected = true) => ({ getCallUUID: () => uuid, isConnected: () => connected, sendDtmf: (d: string) => void sent.push(d) });
  const held = { uuid: "leg-1", conversation: "conv-a" };
  assert.equal(carries(phone(null), null, "conv-a"), false, "after a reload mid-call, or a call taken elsewhere, the keypad is not offered");
  assert.equal(carries(phone("leg-1"), held, "conv-b"), false, "a card beside the answered call, as one in wrap-up or another ringing, gets no keypad");
  assert.equal(carries(phone("leg-2"), held, "conv-a"), false, "a session the desk did not answer is not this card's call");
  assert.match(tone(phone("leg-1"), held, "conv-b", true, "5"), /no longer holds/);
  assert.match(tone(phone("leg-1"), held, "conv-a", false, "5"), /lost its connection/, "an offline browser sends nothing");
  assert.match(tone(phone("leg-1", false), held, "conv-a", true, "5"), /lost its connection/, "nor one whose phone lost its signaling connection");
  assert.deepEqual(sent, [], "a refused key never reaches the phone");
  assert.equal(tone(phone("leg-1"), held, "conv-a", true, "5"), "");
  assert.deepEqual(sent, ["5"]);
});
