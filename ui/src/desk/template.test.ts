import assert from "node:assert/strict";
import { test } from "node:test";
import { fill, holes } from "./template.ts";

const shipped = {
  id: "t1",
  name: "shipped",
  language: "en_US",
  namespace: "waba1-shipped-en_US",
  components: [
    { type: "HEADER", format: "IMAGE" },
    { type: "BODY", text: "Hi {{1}}, order {{2}} shipped" },
    { type: "FOOTER", text: "Acme Support" },
    { type: "BUTTONS", buttons: [{ text: "Call us" }, { text: "Track", url: "https://t.example/{{1}}" }] },
  ],
};

test("a WhatsApp template asks for each of its variables and is sent filled as the service checks it", () => {
  assert.deepEqual(holes(shipped).map((h) => h.label), ["Header image URL", "Body {{1}}", "Body {{2}}", "Track link {{1}}"]);
  assert.deepEqual(fill(shipped, ["https://m.example/box.png", "Ana", "A-1", "A-1"]), {
    name: "shipped",
    language: "en_US",
    namespace: "waba1-shipped-en_US",
    components: [
      { type: "header", parameters: [{ type: "media", media: "https://m.example/box.png" }] },
      { type: "body", parameters: [{ type: "text", text: "Ana" }, { type: "text", text: "A-1" }] },
      { type: "button", sub_type: "url", index: "1", parameters: [{ type: "text", text: "A-1" }] },
    ],
  });
  const plain = { ...shipped, components: [{ type: "HEADER", format: "TEXT", text: "Order {{1}}" }, { type: "BODY", text: "It shipped." }] };
  assert.deepEqual(fill(plain, ["A-1"]).components, [{ type: "header", parameters: [{ type: "text", text: "A-1" }] }], "a part with no variable sends no parameters");
});
