import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { type ViteDevServer, createServer } from "vite";

let vite: ViteDevServer;
before(async () => {
  Object.assign(globalThis, { window: globalThis });
  vite = await createServer({ server: { middlewareMode: true, hmr: false, ws: false }, appType: "custom", logLevel: "silent" });
});
after(() => vite.close());

test("after registering, the panel shows the organization key and the email you sign in with, as HS stored it", async () => {
  const { NewKey } = await vite.ssrLoadModule("/src/signin.tsx");
  const html = renderToStaticMarkup(createElement(NewKey, { shown: { key: "hs_live_1", email: "ADA@example.com" }, onDone() {} }));
  assert.match(html, /value="hs_live_1"/);
  assert.match(html, /You sign in as <span[^>]*>ADA@example.com<\/span>/);
});

test("a click on a link inside the console changes the route without loading a page, which would end the desk's call", async () => {
  const pushed: string[] = [];
  const told: string[] = [];
  class Element {
    href: string;
    constructor(href: string) {
      this.href = href;
    }
    closest = () => (this.href.startsWith("/") ? this : null);
    getAttribute = () => this.href;
  }
  Object.assign(globalThis, { Element, PopStateEvent: Event, history: { pushState: (_: unknown, __: string, url: string) => pushed.push(url) }, dispatchEvent: (e: Event) => told.push(e.type) });
  const { follow } = await vite.ssrLoadModule("/src/shell.tsx");
  const click = (href: string, more = {}) => {
    let kept = false;
    follow({ target: new Element(href), button: 0, preventDefault: () => (kept = true), ...more });
    return kept;
  };
  assert.equal(click("/teams"), true, "the browser does not load /teams");
  assert.deepEqual([pushed, told], [["/teams"], ["popstate"]]);
  assert.equal(click("/teams", { metaKey: true }), false, "a link opened in a new tab is the browser's");
  assert.equal(click("/teams", { button: 1 }), false);
  assert.equal(click("https://example.com/"), false, "a link off the console is the browser's");
  assert.deepEqual(pushed, ["/teams"]);
});

test("the handoff block draws the bot's and the customer's turns in order with who said each, and nothing without a transcript", async () => {
  const { Handoff } = await vite.ssrLoadModule("/src/ui.tsx");
  const turns = [
    { role: "bot", text: "Hi, this is Acme Support.", at: "2026-11-17T10:00:00Z" },
    { role: "customer", text: "My blender jar arrived cracked.", at: null },
  ];
  const html = renderToStaticMarkup(createElement(Handoff, { turns }));
  assert.match(html, /Before the handoff.*Bot<\/span>Hi, this is Acme Support\.<time dateTime="2026-11-17T10:00:00Z".*Customer<\/span>My blender jar arrived cracked\.<\/li>/);
  assert.equal(html.match(/<time/g)?.length, 1, "a turn without a time shows none");
  for (const none of [null, undefined, []]) assert.equal(renderToStaticMarkup(createElement(Handoff, { turns: none })), "", `nothing for ${JSON.stringify(none)}`);
});

test("a conversation's wrap-up shows each agent's disposition with the note written then, and no note line without one", async () => {
  const { WrapUp } = await vite.ssrLoadModule("/src/conversations.tsx");
  const rows = [
    { agent_id: "a1", disposition: "Resolved", sub_disposition: "Replacement sent", skipped: false, note: "New jar ships today." },
    { agent_id: null, disposition: "Dropped", skipped: false, note: null },
  ];
  const html = renderToStaticMarkup(createElement(WrapUp, { rows, name: (id: string | null, fallback: string) => (id ? "Bo Carter" : fallback) }));
  assert.match(html, /Bo Carter: Resolved, Replacement sent<p[^>]*>New jar ships today\.<\/p><\/li><li>Automatic: Dropped<\/li>/);
});
